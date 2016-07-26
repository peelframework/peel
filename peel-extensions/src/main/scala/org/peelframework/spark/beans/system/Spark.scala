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
package org.peelframework.spark.beans.system

import java.lang.{System => Sys}
import java.nio.file.Paths
import java.util.regex.Pattern

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{LogCollection, SetUpTimeoutException, System}
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.console.ConsoleColorise
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.util.matching.Regex

/** Wrapper class for Spark.
  *
  * Implements Spark as a Peel `System` and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`.
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Spark(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("spark", version, configKey, lifespan, dependencies, mc)
                                       with LogCollection {

  // ---------------------------------------------------
  // LogCollection.
  // ---------------------------------------------------

  var latestEventLogBeforeRun: Option[String] = None

  def eventLogPattern() = {
    val logDir = config.getString(s"system.$configKey.path.log")
    s"$logDir/app-*"
  }

  /** The patterns of the log files to watch. */
  override protected def logFilePatterns(): Seq[Regex] = {
    val user = Pattern.quote(config.getString(s"system.$configKey.user"))
    config.getStringList(s"system.$configKey.config.slaves").asScala.map(Pattern.quote).flatMap(slave => Seq(
      s"spark-$user-.+-$slave\\.log".r,
      s"spark-$user-.+-$slave\\.out".r))
  }

  override def beforeRun(run: Run[System]): Unit = {
    // delegate to parent
    super[LogCollection].beforeRun(run)
    // custom logic for Spark
    try {
      latestEventLogBeforeRun = for {
        f <- (shell !! s"ls -t ${eventLogPattern()}").split(Sys.lineSeparator).headOption.map(_.trim)
      } yield f
    } catch {
      case e: Exception =>
        latestEventLogBeforeRun = None
    }
  }

  override def afterRun(run: Run[System]): Unit = {
    // delegate to parent
    super[LogCollection].afterRun(run)
    // custom logic for Spark
    val eventLog = for {
      f <- (shell !! s"ls -t ${eventLogPattern()}").split(Sys.lineSeparator).headOption.map(_.trim)
    } yield f
    if (eventLog.isEmpty || eventLog == latestEventLogBeforeRun) {
      logger.warn("No event log created for experiment".yellow)
    }
    else {
      shell ! s"cp ${eventLog.head} ${run.home}/logs/$name/$beanName/${Paths.get(eventLog.head).getFileName}"
    }
  }

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  /** Parses the configuration and generates the Spark configuration files (slaves, spark-env.sh, spark-defaults.conf)
    * from the moustache templates
    *
    * @return System configuration for this system.
    */
  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    //rename template files - strip the .template
    // TODO: this needs to be done only on demand
    shell ! s"""if [ ! -e "$conf/spark-env.sh" ]; then mv "$conf/spark-env.sh.template" "$conf/spark-env.sh"; fi """
    shell ! s"""if [ ! -e "$conf/spark-defaults.conf" ]; then mv "$conf/spark-defaults.conf.template" "$conf/spark-defaults.conf"; fi """
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env](s"system.$configKey.config.env", s"$conf/spark-env.sh", templatePath("conf/spark-env.sh"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.defaults", s"$conf/spark-defaults.conf", templatePath("conf/spark-defaults.conf"), mc),
      SystemConfig.Entry[Model.Yaml](s"system.$configKey.config.log4j", s"$conf/log4j.properties", templatePath("conf/log4j.properties"), mc)
    )
  })

  /** Starts up the system and polls to check whether everything is up.
    *
    * This methods attempts to set up and start the system on all specified nodes.
    * If an attempt fails, we retry to start it `\${system.spark.startup.max.attempts}` times or shut down the system
    * after exceeding this limit.
    *
    * @throws SetUpTimeoutException If the system was not brought after `\${system.spark.startup.max.attempts}` or
    *                               `\${startup.pollingCounter}` times `\${startup.pollingInterval}` milliseconds.
    */
  override def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")

    val init = (host: String, paths: Seq[String]) => {
      val cmd = paths.map(path => s"rm -Rf $path && mkdir -p $path").mkString(" && ")
      s""" ssh $user@$host "$cmd" """
    }

    val hosts = config.getStringList(s"system.$configKey.config.slaves").asScala
    val paths = config.getString(s"system.$configKey.config.defaults.spark.local.dir").split(',')
    val workDir = config.getString(s"system.$configKey.path.work")

    val futureInitOps = Future.traverse(hosts){ host =>
      for {
        _ <- Future {
          logger.info(s"Initializing Spark tmp directories '${paths.mkString(",")}' at $host")
          shell ! (init(host, paths), s"Unable to initialize Spark tmp directories '${paths.mkString(",")}' at $host.")
        }
        f <- Future {
          logger.debug(s"Removing Spark work directory content '$workDir' at $host")
          // we ignore the failure here cause they are (most likely) due to 'stale file handles' in shared folders
          // aka. shared files are deleted concurrently
          shell ! rmWorkDir(user, host, workDir)
        }
      } yield f
    }

    // await for all futureInitOps to finish
    Await.result(futureInitOps, Math.max(30, 5 * hosts.size).seconds)

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.slaves").size()
        val init = 0 // Spark resets the job manager log on startup

        shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/start-all.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/spark-$user-org.apache.spark.deploy.master.Master-*.out | grep 'Registering worker' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/stop-all.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override def stop(): Unit = {
    shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/stop-all.sh"
    isUp = false

    // remove work dir; as in start
    val user = config.getString(s"system.$configKey.user")
    val hosts = config.getStringList(s"system.$configKey.config.slaves").asScala
    val workDir = config.getString(s"system.$configKey.path.work")

    val futureOps = Future.traverse(hosts) { host =>
      Future {
        logger.debug(s"Removing Spark work directory content '$workDir' at $host")
        shell ! rmWorkDir(user, host, workDir)
      }
    }

    Await.result(futureOps, Math.max(30, 5 * hosts.size).seconds)
  }

  def isRunning = {
    val pidDir = config.getString(s"system.$configKey.config.env.SPARK_PID_DIR")
    (shell ! s""" ps -p `cat $pidDir/spark-*Master*.pid` """) == 0 ||
      (shell ! s""" ps -p `cat $pidDir/spark-*Worker*.pid` """) == 0
  }

  private def rmWorkDir(user: String, host: String, workDir: String) = {
    val cmd = s""" rm -Rf $workDir/* """
    s""" ssh $user@$host "$cmd" """
  }
}
