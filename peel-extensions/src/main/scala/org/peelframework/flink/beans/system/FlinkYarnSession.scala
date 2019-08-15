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

/** Wrapper class for a Flink YARN session.
  *
  * Implements a Flink YARN session as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class FlinkYarnSession(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends Flink(version, configKey, lifespan, dependencies, mc) {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  override def hosts: Seq[String] = Seq(config.getString(s"system.$configKey.config.yaml.jobmanager.rpc.address"))

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex] = {
    val user = Pattern.quote(config.getString(s"system.$configKey.user"))
    hosts.map(Pattern.quote).flatMap(host => Seq(
      s"flink-$user-yarn-session-\\d+-$host\\.log".r,
      s"flink-$user-yarn-session-\\d+-$host\\.out".r))
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml](s"system.$configKey.config.yaml", s"$conf/flink-conf.yaml", templatePath("conf/flink-conf.yaml"), mc),
      SystemConfig.Entry[Model.Yaml](s"system.$configKey.config.log4j", s"$conf/log4j-yarn-session.properties", templatePath("conf/log4j-yarn-session.properties"), mc)
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
        var done = false

        shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/yarn-session.sh -d"

        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (!done) {
          logger.info(s"Waiting for session to start")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          done = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-yarn-session-*.log | grep 'YARN application has been deployed successfully.' | wc -l""").trim) == 1
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        // Flink yarn-session.sh does not create the Flink PID directory (that happens in config.sh and flink-daemon.sh).
        // However, the application ID is stored in /tmp/.yarn-properties-$user
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            // should Peel try to stop the system here?
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            shell ! s"""rm -f /tmp/.yarn-properties-$user"""
            throw e
          }
      }
    }
  }

  override protected def stop(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val appId = (shell !! s"""grep applicationID /tmp/.yarn-properties-$user | sed -e 's/applicationID=\\(.*\\).*/\\1/'""").trim()
    shell ! s"""echo quit | ${config.getString(s"system.$configKey.path.home")}/bin/yarn-session.sh -id $appId"""
    if (isRunning) {
      logger.warn(s"Flink YARN session still appears to be running after attempted shutdown (file /tmp/.yarn-properties-$user exists)")
      shell ! s"rm -f /tmp/.yarn-properties-$user"
    }
    isUp = false
  }

  def isRunning: Boolean = {
    // maybe query YARN rest API
    val user = config.getString(s"system.$configKey.user")
    (shell ! s"""ls /tmp/.yarn-properties-$user""") == 0
  }
}
