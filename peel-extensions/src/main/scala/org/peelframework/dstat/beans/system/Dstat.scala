/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.dstat.beans.system

import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell
import resource._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
 * Implements Dstat as a Peel system.
 *
 * The options for the `dstat` command can be configured using `system.${configKey}.config.cli`.
 * Per default, only the `total` values are reported for CPU, network, and disk usage:
 *
 * @param version      Version of the system (e.g. "7.1")
 * @param configKey    The system configuration resides under `system.${configKey}`
 * @param lifespan     `Lifespan` of the system
 * @param dependencies Set of dependencies that this system needs
 * @param mc           The moustache compiler to compile the templates that are used to generate property files for the system
 */
class Dstat(
  version: String,
  configKey: String,
  lifespan: Lifespan,
  dependencies: Set[System] = Set(),
  mc: Mustache.Compiler) extends System("dstat", version, configKey, lifespan, dependencies, mc) {

  def master = config.getString(s"system.$configKey.config.master")

  def slaves = config.getStringList(s"system.$configKey.config.slaves").asScala.toSet

  def hosts = slaves + master

  var processes = Set.empty[ProcessDescriptor]

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override protected def configuration(): SystemConfig = SystemConfig(config, {
    val home = config.getString(s"system.$configKey.path.home")
    List(
      SystemConfig.Entry[Model.HOCON](s"system.$configKey.config", s"$home/config.conf", templatePath("conf/config.conf"), mc)
    )
  })

  override protected def start(): Unit = {
    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))
    val outDir = Paths.get(config.getString(s"system.$configKey.config.out.dir"))

    // ensure that the PID file is writable
    // otherwise, create an empty file or throw an error if this is not possible
    if (!Files.exists(pidFle)) {
      Files.createDirectories(pidFle.getParent)
      shell.touch(pidFle.toString)
    }

    // ensure that the PID file is empty
    processes = for {
      line <- scala.io.Source.fromFile(pidFle.toString).getLines().toSet[String]
      desc <- ProcessDescriptor.unapply(line)
    } yield desc

    if (processes.nonEmpty) throw new RuntimeException(Seq(
      "It appears that some dstat processes are still running on the following machines:",
      processes.map(desc => s"  - ${desc.host} with pid ${desc.pid}").mkString("\n"),
      "Please stop them first (e.g. using the `sys:teardown` command).").mkString("\n"))

    // ensure that the out dirs exist on all hosts
    logger.info("Ensuring that output dirs exists on all hosts")
    val futureEnsureOutDirs = Future.traverse(hosts)(host => Future {
      shell !(
        s""" ssh $host 'mkdir -p "$outDir"' """, fatal = true,
        errorMsg = s"Cannot ensure that output dir '$outDir' exists on host '$host'")
      logger.info(s"Ensured that output dir '$outDir' exists on host '$host'")
    })
    Await.result(futureEnsureOutDirs, (5 * hosts.size).seconds)
  }

  override protected def stop(): Unit =
    stopProcesses(None)

  override def isRunning: Boolean =
    processes.size == hosts.size

  override def beforeRun(run: Run[System]): Unit =
    startProcesses(run)

  override def afterRun(run: Run[System]): Unit =
    stopProcesses(Some(run))

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  private def startProcesses(run: Run[System]): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val dstat = s"${config.getString(s"system.$configKey.path.home")}/dstat"

    val outDir = Paths.get(config.getString(s"system.$configKey.config.out.dir"))

    val cpus = config.getStringList(s"system.$configKey.config.cli.cpu.list").asScala
    val nets = config.getStringList(s"system.$configKey.config.cli.net.list").asScala
    val dsks = config.getStringList(s"system.$configKey.config.cli.disk.list").asScala
    val xtra = config.getString(s"system.$configKey.config.cli.extra")
    val itvl = config.getInt(s"system.$configKey.config.cli.interval")

    logger.info("Starting dstat processes on all hosts")
    val futureProcessDescriptors = Future.traverse(hosts)(host => Future {
      val out = s"$outDir/dstat-$user-$host-${run.name}.csv"
      val cmd = Seq(dstat,
        s"--epoch",
        s"--cpu -C ${cpus mkString ","}",
        s"--mem",
        s"--net -N ${nets mkString ","}",
        s"--disk -D ${dsks mkString ","}",
        xtra,
        s"--noheaders",
        s"--nocolor",
        s"--output $out",
        itvl) mkString " "

      val pid = (shell !! s""" ssh $host 'rm -f "$out"; nohup $cmd >/dev/null 2>/dev/null & echo $$!' """).trim
      logger.info(s"Dstat started on host '$host' with PID $pid")

      ProcessDescriptor(host, pid.toInt)
    })
    setProcesses(Await.result(futureProcessDescriptors, (5 * hosts.size).seconds))
  }

  private def stopProcesses(runOpt: Option[Run[System]]): Unit = {
    val user = config.getString(s"system.$configKey.user")

    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))
    val outDir = Paths.get(config.getString(s"system.$configKey.config.out.dir"))

    if (Files.exists(pidFle) && processes.isEmpty)
      processes = for {
        line <- scala.io.Source.fromFile(pidFle.toString).getLines().toSet[String]
        desc <- ProcessDescriptor.unapply(line)
      } yield desc

    val logDirOpt = for (run <- runOpt) yield {
      val logDir = Paths.get(run.home, "logs", name, beanName)

      if (!Files.exists(logDir)) {
        Files.createDirectories(logDir)
        logger.info(s"Ensuring dstat results folder '$logDir' exists")
      }

      logDir
    }

    logger.info("Stopping dstat processes on all hosts")
    val futureCopyContents = Future.traverse(processes)(desc => {
      for {
        killProcess <- Future {
          shell ! s"ssh ${desc.host} kill ${desc.pid}"
          logger.info(s"Dstat with PID ${desc.pid} stopped on host '${desc.host}'")
        }
        copyContents <- Future {
          for {
            run <- runOpt
            logDir <- logDirOpt
          } {
            val src = s"$outDir/dstat-$user-${desc.host}-${run.name}.csv"
            val dst = s"$logDir/dstat-$user-${desc.host}.csv"

            shell ! s"scp -C ${desc.host}:$src $dst"
            logger.info(s"Copying data from '${desc.host}:$src' to $dst")
          }

          desc
        }
      } yield copyContents
    })
    setProcesses(processes diff Await.result(futureCopyContents, (5 * hosts.size).seconds))
  }

  private def setProcesses(processes: Set[ProcessDescriptor]) = {
    val pidFle = Paths.get(config.getString(s"system.$configKey.path.pids"))

    import StandardCharsets.UTF_8
    import StandardOpenOption.{CREATE, WRITE, TRUNCATE_EXISTING}

    for {
      buf <- managed(Files.newBufferedWriter(pidFle, UTF_8, CREATE, WRITE, TRUNCATE_EXISTING))
      out <- managed(new PrintWriter(buf))
    } for (desc <- processes) {
      out.write(s"$desc\n")
    }

    this.processes = processes
  }
}

case class ProcessDescriptor(host: String, pid: Int)

object ProcessDescriptor {
  val ProcessDescriptorRegex = "ProcessDescriptor\\((.*),(\\d+)\\)".r

  def unapply(s: String): Option[ProcessDescriptor] = s match {
    case ProcessDescriptorRegex(host, pid) => Some(new ProcessDescriptor(host, pid.toInt))
    case _ => None
  }
}