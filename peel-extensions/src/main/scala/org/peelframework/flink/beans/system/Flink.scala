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

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{LogCollection, SetUpTimeoutException, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/** Wrapper class for Flink.
  *
  * Implements Flink as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Flink(
  version      : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("flink", version, lifespan, dependencies, mc)
                                       with LogCollection {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex] = {
    List("flink-.+\\.log".r, "flink-.+\\.out".r)
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.flink.path.config")
    List(
      SystemConfig.Entry[Model.Hosts]("system.flink.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml]("system.flink.config.yaml", s"$conf/flink-conf.yaml", templatePath("conf/flink-conf.yaml"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString("system.flink.user")
    val logDir = config.getString("system.flink.path.log")

    // check if tmp dir exists and create if not
    val tmpDirs = config.getString("system.flink.config.yaml.taskmanager.tmp.dirs")
    val jmHost = config.getString("system.flink.config.yaml.jobmanager.rpc.address")

    for (tmpDir <- tmpDirs.split(':')) {
      logger.info(s"Initializing tmp directory $tmpDir at jobmanager host $jmHost")
      shell ! (s""" ssh $user@$jmHost "rm -Rf $tmpDir" """, "Unable to remove Flink tmp directory.")
      shell ! (s""" ssh $user@$jmHost "mkdir -p $tmpDir" ""","Unable to create Flink tmp directory.")

      for (tmHost <- config.getStringList(s"system.$configKey.config.slaves").asScala) {
        logger.info(s"Initializing tmp directory $tmpDir at taskmanager host $tmHost")
        shell ! (s""" ssh $user@$tmHost "rm -Rf $tmpDir" """, "Unable to remove Flink tmp directory.")
        shell ! (s""" ssh $user@$tmHost "mkdir -p $tmpDir" ""","Unable to create Flink tmp directory.")
      }
    }

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.slaves").size()
        val init = 0 // Flink resets the job manager log on startup

        shell ! s"${config.getString("system.flink.path.home")}/bin/start-cluster.sh"
        shell ! s"${config.getString("system.flink.path.home")}/bin/start-webclient.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.flink.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.flink.startup.polling.interval"))
          // get new values
          if (version.startsWith("0.6"))
            curr = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-jobmanager-*.log | grep 'Creating instance' | wc -l""").trim())
          else
            curr = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-jobmanager-*.log | grep 'Registered TaskManager' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.flink.startup.max.attempts")) {
            shell ! s"${config.getString("system.flink.path.home")}/bin/stop-cluster.sh"
            shell ! s"${config.getString("system.flink.path.home")}/bin/stop-webclient.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString("system.flink.path.home")}/bin/stop-cluster.sh"
    shell ! s"${config.getString("system.flink.path.home")}/bin/stop-webclient.sh"
    isUp = false
  }

  def isRunning = {
    (shell ! s"""ps -ef | grep 'flink' | grep 'java' | grep 'jobmanager' | grep -v 'grep' """) == 0 // TODO: fix using PID
  }
}
