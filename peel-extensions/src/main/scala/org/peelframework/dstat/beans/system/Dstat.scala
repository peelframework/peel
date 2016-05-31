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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{DistributedLogCollection, System}
import org.peelframework.core.config.SystemConfig
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/**
 * Implements Dstat as a Peel system.
 *
 * The options for the `dstat` command can be configured using `system.${configKey}.cli.options`.
 * Per default, if `system.\${configKey}.cli.options` is not set, the options are generated as follows:
 * <p/>
 * The options equal to `--epoch --cpu -C $cpuList --mem --net -N $netList --disk -D $diskList --noheaders --nocolor`
 * where `$cpuList` (`$netList`, `$diskList`) are either substituted by
 * <ul>
 *   <li>`system.${configKey}.cli.cpu.list` (`system.${configKey}.cli.net.list`, `system.${configKey}.cli.disk.list`) if set</li>
 *   <li>or generated otherwise including the `total` statistics as well as all cores (interfaces, devices)</li>
 * </ul>
 *
 * @param version Version of the system (e.g. "7.1")
 * @param configKey The system configuration resides under `system.${configKey}`
 * @param lifespan `Lifespan` of the system
 * @param dependencies Set of dependencies that this system needs
 * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
 */
class Dstat(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("dstat", version, configKey, lifespan, dependencies, mc)
                                       with DistributedLogCollection {

  override def hosts = {
    val master = config.getString("runtime.hostname")
    val slaves = config.getStringList(s"system.$configKey.config.slaves").asScala
    master +: slaves
  }

  override protected def logFilePatterns(): Seq[Regex] = {
    val user = config.getString(s"system.$configKey.user")
    hosts.map(host => Pattern.quote(s"dstat-$user-$host.csv").r)
  }


  override protected def configuration(): SystemConfig = {
    SystemConfig(config, List.empty)
  }

  override protected def stop(): Unit = {
    val pidFile = Paths.get(s"${config.getString(s"system.$configKey.config.pid.dir")}/dstat.pid")
    if (Files.exists(pidFile)) {
      Closeable.guard(Files.newBufferedReader(pidFile, StandardCharsets.UTF_8)) on { reader =>
        while(reader.ready()) {
          val entry = reader.readLine().split(',')
          if (entry.length == 2) {
            shell ! s""" ssh ${entry(0)} "kill ${entry(1)} 2>/dev/null >/dev/null" """
          }
        }
      }

      Files.delete(pidFile)
    }
  }

  override def isRunning: Boolean = {
    val pidDir = Paths.get(s"${config.getString(s"system.$configKey.config.pid.dir")}/dstat.pid")
    Files.exists(pidDir) && (
      Closeable.guard(Files.newBufferedReader(pidDir, StandardCharsets.UTF_8)) on { reader =>
        var ret = true
        while(reader.ready()) {
          val entry = reader.readLine().split(',')
          if (entry.length == 2) {
            ret = ret & (shell ! s""" ssh ${entry(0)} "ps -p ${entry(1)}" """) == 0
          }
        }
        ret
      }
    )
  }

  override protected def start(): Unit = {
    // we start dstat in beforeRun, as in the current workflow
    // beforeRun and then start is invoked which causes missing data (especially the header)
    // the problem is that this forces dstat to be of lifespan RUN
  }

  private def _start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")
    val dstat = config.getString(s"system.$configKey.path.home") + "/dstat"

    val pids = (for (host <- hosts) yield {
      val options = buildOptions(host)
      val cmd = s"$dstat $options --output $logDir/dstat-$user-$host.csv 1"

      logger.debug(s"""Executing "$cmd" on host "$host"""")
      val dstatPid = shell !! s""" ssh $host "nohup $cmd >/dev/null 2>/dev/null & echo \\$$!" """
      logger.debug(s"Dstat started on $host with PID $dstatPid")

      (host, dstatPid)
    }).toMap

    savePids(pids)
  }

  override def beforeRun(run: Run[System]): Unit = {
    // make sure log folder exists
    val logDir = config.getString(s"system.$configKey.path.log")
    shell.ensureFolderIsWritable(Paths.get(logDir))
    // make sure the files exist before calling beforeRun
    touch()
    // delegate to parent
    super[DistributedLogCollection].beforeRun(run)
    _start()
  }

  /**
   * Generates the options for dstat if not set.
   * Heavily inspired by <a href="https://github.com/mvneves/dstat-monitor">https://github.com/mvneves/dstat-monitor</a>.
   *
   * @param host the machine for which the options should be generated.
   * @return the options string.
   */
  def buildOptions(host: String): String = {
    val dstat = config.getString(s"system.$configKey.path.home") + "/dstat"

    // if options is set, then use it
    val options = config.getString(s"system.$configKey.cli.options")
    if (options != "") {
      return options
    }

    // if options is empty, then build the options
    var cpuList = config.getString(s"system.$configKey.cli.cpu.list")
    if (cpuList == "") {
      val cpuListCmd = s"$dstat --cpu --full --nocolor 1 0 | head -n 1 | tr -d '-' | tr ' ' '\\n' | wc -l"
      val nCpu = Integer.parseInt((shell !! s""" ssh $host "$cpuListCmd" """).trim)

      if(nCpu == 0) throw new RuntimeException(s"Running dstat failed on $host. Command was\n$cpuListCmd")

      cpuList = s"total,${(0 until nCpu).mkString(",")}"
    }

    var netList = config.getString(s"system.$configKey.cli.net.list")
    if (netList == "") {

      val netListCmd = s"$dstat --net --full --nocolor 1 0 | head -n 1 | tr -d '-' | sed 's/net\\///g'"
      val netListRes = shell !! s""" ssh $host "$netListCmd" """

      val pattern = """.*eth([0-9]*).*""".r
      netList = "total," + netListRes.split("\\s+").map { case pattern(num) => s"eth$num"; case s => s }.reduce((s1, s2) => s"$s1,$s2")
    }

    var diskList = config.getString(s"system.$configKey.cli.disk.list")
    if (diskList == "") {
      val diskListCmd = s"$dstat --disk --full --nocolor --noupdate 1 0 | head -n 1 | sed 's/ /\\n/g' | sed -r 's/.*dsk\\/([^-]*).*/\\1/g' | tr '\\n' ' '"
      val diskListRes = shell !! s""" ssh $host "$diskListCmd" """
      diskList = "total," + diskListRes.split("\\s+").reduce((s1, s2) => s"$s1,$s2")
    }

    s"--epoch --cpu -C $cpuList --mem --net -N $netList --disk -D $diskList --noheaders --nocolor"
  }

  private def savePids(pids: Map[String, String]) = {
    val pidDir = Paths.get(config.getString(s"system.$configKey.config.pid.dir"))
    if (!Files.isDirectory(pidDir)) {
      Files.createDirectory(pidDir)
    }
    val pidFile = Paths.get(pidDir.toString, "dstat.pid")
    if (Files.exists(pidFile)) {
      logger.warn("Dstat pid file exists: Try to shut down already running Dstat.")
      stop()
    }
    Closeable.guard { Files.newBufferedWriter(pidFile, StandardCharsets.UTF_8) } on { writer =>
      for ((host, pid) <- pids) {
        writer.write(s"$host,$pid")
        writer.newLine()
      }
    }
  }

  private def touch(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")
    for (host <- hosts) yield {
      val cmd = s"touch $logDir/dstat-$user-$host.csv"
      shell ! s""" ssh $host "$cmd" """
    }
  }

  object Closeable {
    class Guard[A <: AutoCloseable](closeable: A) {
      def on[B](block: A => B) = {
        var t: Throwable = null
        try {
          block(closeable)
        } catch {
          case e: Exception => t = e; throw t
        } finally {
          if (closeable != null) {
            if (t != null) {
              try {
                closeable.close()
              } catch {
                case e: Throwable => t.addSuppressed(e)
              }
            } else {
              closeable.close()
            }
          }
        }
      }
    }

    def guard[A <: AutoCloseable](closable: A) = new Guard[A](closable)
  }
}
