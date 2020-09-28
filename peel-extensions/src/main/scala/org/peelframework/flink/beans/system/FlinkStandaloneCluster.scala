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
package org.peelframework.flink.beans.system

import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{DistributedLogCollection, SetUpTimeoutException, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.{Version, shell}

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.matching.Regex

/** Wrapper class for Flink.
  *
  * Implements a Flink standalone cluster as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class FlinkStandaloneCluster(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends Flink(version, configKey, lifespan, dependencies, mc) {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  override def hosts = {
    val master = config.getString(s"system.$configKey.config.yaml.jobmanager.rpc.address")
    val slaves = config.getStringList(s"system.$configKey.config.slaves").asScala
    master +: slaves
  }

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex] = {
    val user = Pattern.quote(config.getString(s"system.$configKey.user"))
    hosts.map(Pattern.quote).flatMap(host => Seq(
      s"flink-$user-standalonesession-\\d+-$host\\.log".r,
      s"flink-$user-standalonesession-\\d+-$host\\.out".r,
      s"flink-$user-jobmanager-\\d+-$host\\.log".r,
      s"flink-$user-jobmanager-\\d+-$host\\.out".r,
      s"flink-$user-taskmanager-\\d+-$host\\.log".r,
      s"flink-$user-taskmanager-\\d+-$host\\.out".r))
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml](s"system.$configKey.config.yaml", s"$conf/flink-conf.yaml", templatePath("conf/flink-conf.yaml"), mc),
      SystemConfig.Entry[Model.Yaml](s"system.$configKey.config.log4j", s"$conf/log4j.properties", templatePath("conf/log4j.properties"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")

    val init = (host: String, paths: Seq[String]) => {
      val cmd = paths.map(path => s"rm -Rf $path && mkdir -p $path").mkString(" && ")
      s""" ssh $user@$host "$cmd" """
    }

    val hosts = config.getStringList(s"system.$configKey.config.slaves").asScala
    val paths = config.getString(s"system.$configKey.config.yaml.taskmanager.tmp.dirs").split(':')

    val futureInitOps = Future.traverse(hosts)(host => Future {
      logger.info(s"Initializing Flink tmp directories '${paths.mkString(":")}' at $host")
      shell ! (init(host, paths), s"Unable to initialize Flink tmp directories '${paths.mkString(":")}' at $host.")
    })

    // await for all futureInitOps to finish
    Await.result(futureInitOps, Math.max(30, 5 * hosts.size).seconds)

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.slaves").size()
        val init = 0 // Flink resets the job manager log on startup

        shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/start-cluster.sh"
        shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/start-webclient.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          if (Version(version) < Version("0.6")) {
            curr = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-jobmanager-*.log | grep 'Creating instance' | wc -l""").trim())
          } else if (Version(version) < Version("1.6")) {
            curr = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-jobmanager-*.log | grep 'Registered TaskManager' | wc -l""").trim())
          } else {
            curr = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-standalonesession-*.log | grep 'Registering TaskManager' | wc -l""").trim())
          }
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/stop-cluster.sh"
            shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/stop-webclient.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/stop-cluster.sh"
    shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/stop-webclient.sh"
    shell ! s"rm -f ${config.getString(s"system.$configKey.config.yaml.env.pid.dir")}/flink-*.pid"
    isUp = false
  }

  def isRunning = {
    (shell ! s"""ps -p `cat ${config.getString(s"system.$configKey.config.yaml.env.pid.dir")}/flink-*.pid`""") == 0
  }
}
