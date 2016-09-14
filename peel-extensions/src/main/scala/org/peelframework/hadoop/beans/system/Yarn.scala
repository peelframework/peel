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

import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{LogCollection, SetUpTimeoutException, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.util.matching.Regex

/** Wrapper class for Yarn.
  *
  * Implements Yarn as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Yarn(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("yarn", version, configKey, lifespan, dependencies, mc)
                                       with LogCollection {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex] = {
    // TODO: rework based on http://hortonworks.com/blog/simplifying-user-logs-management-and-access-in-yarn/
    val user = Pattern.quote(config.getString(s"system.$configKey.user"))
    config.getStringList(s"system.$configKey.config.slaves").asScala.map(Pattern.quote).map(slave =>
      s"yarn-$user-resourcemanager-$slave\\.log".r)
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env](s"system.$configKey.config.env", s"$conf/hadoop-env.sh", templatePath("conf/hadoop-env.sh"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.core", s"$conf/core-site.xml", templatePath("conf/site.xml"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.yarn", s"$conf/yarn-site.xml", templatePath("conf/site.xml"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.mapred", s"$conf/mapred-site.xml", templatePath("conf/site.xml"), mc)
    )
  })

  override def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")

    var failedStartUpAttempts = 0
    while(!isUp) {
      try {
        val total = config.getStringList(s"system.$configKey.config.slaves").size()
        // yarn does not reset the resourcemanagers log at startup
        val init = Integer.parseInt((shell !! s"""cat $logDir/yarn-$user-resourcemanager-*.log | grep 'registered with capability:' | wc -l""").trim())

        shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/start-yarn.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (curr - init < total) {
          logger.info(s"Connected ${curr - init} from $total nodes")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/yarn-$user-resourcemanager-*.log | grep 'registered with capability:' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        logger.info(s"Connected ${curr - init} from $total nodes")
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            stop()
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override def stop(): Unit = {
    shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/stop-yarn.sh"
    isUp = false
  }

  def isRunning = {
    (shell ! s""" ps -p `cat ${config.getString(s"system.$configKey.config.env.YARN_PID_DIR")}/yarn-*-resourcemanager.pid` """) == 0 ||
    (shell ! s""" ps -p `cat ${config.getString(s"system.$configKey.config.env.YARN_PID_DIR")}/yarn-*-nodemanager.pid` """) == 0
  }
}
