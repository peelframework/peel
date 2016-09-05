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
package org.peelframework.core.beans.system

import java.nio.file.{Files, Paths}
import java.io.File
import java.util.concurrent.TimeUnit

import com.samskivert.mustache.Mustache
import com.samskivert.mustache.Mustache.Compiler
import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.config.{Configurable, SystemConfig}
import org.peelframework.core.graph.Node
import org.peelframework.core.util.{Version, shell}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try
import scala.concurrent.ExecutionContext.Implicits.global

/** This class represents a System in the Peel framework.
  *
  * Most nodes in the Peel dependency-graph are systems. A [[System]] can specify it's dependencies which are then set
  * up and torn down automatically, according to their [[Lifespan]] values.
  *
  * @param name The name of this bean. Deafults to the system name (e.g. "Flink")
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`.
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
abstract class System(
    val name         : String,
    val version      : String,
    val configKey    : String,
    val lifespan     : Lifespan,
    val dependencies : Set[System],
    val mc           : Mustache.Compiler) extends Node with Configurable with BeanNameAware {

  import scala.language.postfixOps

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  var isUp = lifespan == Lifespan.PROVIDED

  /** The name of this bean. Deafults to the system name. */
  var beanName = name

  /** Creates a complete system installation with updated configuration and starts the system. */
  def setUp(): Unit = {
    if (isRunning) {
      if (isUp)
        logger.info(s"System '$toString' is already up and running")
      else
        logger.warn(s"System '$toString' appears to be running, but is not marked as 'up'. Shut down manually '$toString' or set its bean lifecycle to PROVIDED.")
    } else {
      logger.info(s"Starting system '$toString'")

      if (!Files.exists(Paths.get(config.getString(s"system.$configKey.path.home")))) {
        materializeHome()
      }

      configuration().update()
      if (config.getBoolean(s"system.$configKey.path.isShared"))
        copyHomeToSlaves()
      start()

      logger.info(s"System '$toString' is now up and running")
    }
  }

  def copyHomeToSlaves(): Unit = {
    val homePath = config.getString(s"system.$configKey.path.home")
    val destinationPath = new File(homePath).getParent
    val slaves = config.getStringList(s"system.$configKey.config.slaves").asScala
    val user = config.getString(s"system.$configKey.user")
    val logPath = Paths.get(config.getString(s"system.$configKey.path.log"))
    val relativeLogPath = Paths.get(destinationPath).relativize(logPath).normalize
    val futureSyncedDirs = Future.traverse(slaves) { host =>
      for {
        _ <- Future(createRemoteDirectory(destinationPath, user, host))
        f <- Future(copyDirectorytoRemote(homePath, destinationPath, user, host, relativeLogPath.toString + "/*"))
      } yield f
    }

    // await for all future log file counts and convert the result to a map
    Await.result(futureSyncedDirs, Duration(Math.max(30, 5 * slaves.size), TimeUnit.SECONDS))
  }

  def copyDirectorytoRemote(localSource: String, remoteDestination: String, user: String, host: String, exclude: String): Int = {
    val fullDestination: String = s"$user@$host:$remoteDestination"
    val command = s"rsync -a $localSource $fullDestination --exclude $exclude"
    logger.info(command)
    shell ! (command, s"failed to copy $localSource to $fullDestination", fatal = true)
  }

  def createRemoteDirectory(path: String, user: String, host: String): Int = {
    logger.info(s"creating directory $path on remote host $host")
    shell ! (s"ssh $user@$host mkdir -p $path", s"failed to create directory $path on remote host $host", fatal = true)
  }

  /** Cleans up and shuts down the system. */
  def tearDown(): Unit = {
    if (!isRunning) {
      logger.info(s"System '$toString' is already down")
    } else {
      logger.info(s"Tearing down system '$toString'")

      stop()
      awaitShutdown()
    }
  }

  /** Restarts the system if the system configuration has changed. */
  def update(): Unit = {
    val c = configuration()
    if (!c.hasChanged) {
      logger.info(s"System configuration of '$toString' did not change")
    } else {
      logger.info(s"System configuration of '$toString' changed, restarting...")

      tearDown()
      setUp()

    }
  }

  /** Executed before each experiment run that depends on this system. */
  def beforeRun(run: Run[System]): Unit = {
  }

  /** Executed after each experiment run that depends on this system. */
  def afterRun(run: Run[System]): Unit = {
  }

  /** Waits until the system is shut down (blocking). */
  private def awaitShutdown(): Unit = {
    var maxAttempts = config.getInt("system.default.stop.max.attempts")
    val wait = config.getInt("system.default.stop.polling.interval")
    while (isRunning) {
      // wait a bit
      Thread.sleep(wait)
      if (maxAttempts <= 0) {
        throw new RuntimeException(s"Unable to shut down system '$toString' in time (waited ${config.getInt("system.default.stop.max.attempts") * wait} ms).")
      }
      maxAttempts = maxAttempts - 1
    }
    logger.info(s"Shut down system '$toString'.")
  }

  /** Bean name setter.
    *
    * @param n The configured bean name
    */
  override def setBeanName(n: String) = beanName = n

  /** Alias of name.
    *
    * @return name of the bean
    */
  override def toString: String = beanName

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  /** Returns an of the system configuration using the current Config */
  protected def configuration(): SystemConfig

  /** Starts up the system and polls to check whether everything is up.
    *
    * @throws SetUpTimeoutException If the system was not brought after {startup.pollingCounter} times {startup.pollingInterval} milliseconds.
    */
  protected def start(): Unit

  /** Stops the system. */
  protected def stop(): Unit

  /** Checks whether a process   for this system is already running.
    *
    * This is different from the value of `isUp`, as a system can be running, but not yet up and operational (i.e. if
    * not all worker nodes of a distributed have connected).
    *
    * @return True if a system process for this system exists.
    */
  def isRunning: Boolean

  /** Materializes the system home from an archive.
    *
    * Depends on the following system parameters:
    *
    * * `system.$configKey.path.archive.url` - A URL where the system binary archive can be found online (optional).
    * * `system.$configKey.path.archive.md5` - The md5 sum of the system binary archive.
    * * `system.$configKey.path.archive.src` - The path where the system binary archive should be stored locally.
    * * `system.$configKey.path.archive.dst` - The path where the system binary archive should be extracted.
    */
  protected def materializeHome() = {
    val archiveMD5 = BigInt(config.getString(s"system.$configKey.path.archive.md5"), 16)
    val archiveSrc = config.getString(s"system.$configKey.path.archive.src")
    val archiveDst = config.getString(s"system.$configKey.path.archive.dst")

    if (!Files.exists(Paths.get(archiveSrc))) {
      if (config.hasPath(s"system.$configKey.path.archive.url")) {
        val archiveUrl = config.getString(s"system.$configKey.path.archive.url")
        logger.info(s"Downloading archive '$archiveSrc' from '$archiveUrl' (md5: '$archiveMD5')")
        shell.download(archiveUrl, archiveSrc, archiveMD5)
      }
      else {
        throw new RuntimeException(s"Cannot lazy-load archive for system '$configKey'. Please set an 'archive.url' configuration value.")
      }
    } else {
      logger.info(s"Validating archive '$archiveSrc' (md5: '$archiveMD5')")
      shell.checkMD5(archiveSrc, archiveMD5)
    }

    logger.info(s"Extracting archive '$archiveSrc' to '$archiveDst'")
    shell.extract(archiveSrc, archiveDst)

    logger.info(s"Changing owner of '${config.getString(s"system.$configKey.path.home")}' to ${config.getString(s"system.$configKey.user")}:${config.getString(s"system.$configKey.group")}")
    shell ! "chown -R %s:%s %s".format(
      config.getString(s"system.$configKey.user"),
      config.getString(s"system.$configKey.group"),
      config.getString(s"system.$configKey.path.home"))
  }

  /** Returns the template path closest to the given system and version. */
  protected def templatePath(path: String) = {
    // find closest version prefix with existing template path
    val prefix = Version(version).prefixes.find(prefix => {
      Option(this.getClass.getResource(s"/templates/$configKey/$prefix/$path.mustache")).isDefined
    })

    prefix
      .map(p => s"/templates/$configKey/$p/$path.mustache") // template path for closest version prefix
      .getOrElse(s"/templates/$configKey/$path.mustache") // base template path
  }
}

object System {

  def unapply(x: Any): Option[System] = x match {
    case s: System => Some(s)
    case _ => None
  }
}