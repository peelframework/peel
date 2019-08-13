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
package org.peelframework.hadoop.beans.system

import java.net.URI
import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{DistributedLogCollection, SetUpTimeoutException, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.matching.Regex

/** Wrapper class for HDFS3.
  *
  * Implements HDFS3 as a Peel `System` and provides setup and teardown methods.
  * Additionally it offers the Filesysem capabilities to interact with hdfs.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`.
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class HDFS3(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("hdfs-3", version, configKey, lifespan, dependencies, mc)
                                       with HDFSFileSystem
                                       with DistributedLogCollection {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  override def hosts = {
    val master = config.getString("runtime.hostname")
    val workers = config.getStringList(s"system.$configKey.config.workers").asScala
    master +: workers
  }

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex]  = {
    val user = Pattern.quote(config.getString(s"system.$configKey.user"))
    hosts.map(Pattern.quote).flatMap(host => Seq(
      s"hadoop-$user-namenode-$host\\.log".r,
      s"hadoop-$user-namenode-$host\\.out".r,
      s"hadoop-$user-datanode-$host\\.log".r,
      s"hadoop-$user-datanode-$host\\.out".r))
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.workers", s"$conf/workers", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env](s"system.$configKey.config.env", s"$conf/hadoop-env.sh", templatePath("conf/hadoop-env.sh"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.core", s"$conf/core-site.xml", templatePath("conf/site.xml"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.hdfs", s"$conf/hdfs-site.xml", templatePath("conf/site.xml"), mc)
    )
  })

  /** Checks if all datanodes have connected and the system is out of safemode. */
  override protected def start(): Unit = {
    if (config.getBoolean(s"system.$configKey.format")) format()

    val user = config.getString(s"system.$configKey.user")
    val home = config.getString(s"system.$configKey.path.home")
    val logDir = config.getString(s"system.$configKey.path.log")
    val hostname = config.getString("app.hostname")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.workers").size()
        var init = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.log | grep 'registerDatanode:' | wc -l""").trim())

        shell ! s"$home/sbin/start-dfs.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var safe = !(shell !! s"$home/bin/hdfs dfsadmin -safemode get").toLowerCase.contains("off")
        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (curr - init < totl || safe) {
          logger.info(s"Connected ${curr - init} from $totl nodes, safemode is ${if (safe) "ON" else "OFF"}")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          // depending on the log level its either in *.log or *.out (debug)
          curr = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.out | grep 'registerDatanode:' | wc -l""").trim())
          if (curr == 0) {
            curr = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.log | grep 'registerDatanode:' | wc -l""").trim())
          }
          safe = !(shell !! s"$home/bin/hdfs dfsadmin -safemode get").toLowerCase.contains("off")
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (curr - init < 0) init = 0 // protect against log reset on startup
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            shell ! s"$home/sbin/stop-dfs.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
    mkdir(config.getString(s"system.$configKey.path.input"))
  }

  override protected def stop() = {
    shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/stop-dfs.sh"
    if (config.getBoolean(s"system.$configKey.format")) format()
    isUp = false
  }

  def isRunning = {
    val pidDir = config.getString(s"system.$configKey.config.env.HADOOP_PID_DIR")
    (shell ! s""" ps -p `cat $pidDir/hadoop-*-namenode.pid` """) == 0 ||
      (shell ! s""" ps -p `cat $pidDir/hadoop-*-secondarynamenode.pid` """) == 0 ||
      (shell ! s""" ps -p `cat $pidDir/hadoop-*-datanode.pid` """) == 0
  }

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  private def format() = {
    val user = config.getString(s"system.$configKey.user")
    val home = config.getString(s"system.$configKey.path.home")

    logger.info(s"Formatting namenode")
    shell ! (s"$home/bin/hdfs namenode -format -nonInteractive -force", "Unable to format namenode.")

    val init = (host: String, path: String) =>
      s""" ssh $user@$host "rm -Rf $path && mkdir -p $path/current" """

    val list = for {
      host <- config.getStringList(s"system.$configKey.config.workers").asScala
      path <- config.getString(s"system.$configKey.config.hdfs.dfs.datanode.data.dir").split(',')
    } yield (host, new URI(path).getPath)

    val futureInitOps = Future.traverse(list)(((host: String, path: String) => Future {
      logger.info(s"Initializing HDFS data directory '$path' at $host")
      shell ! (init(host, path), s"Unable to initialize HDFS data directory '$path' at $host.")
    }).tupled)

    // await for all futureInitOps to finish
    Await.result(futureInitOps, Math.max(30, 5 * list.size).seconds)
  }
}
