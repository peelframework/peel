package org.peelframework.core.beans.system

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.config.{Configurable, SystemConfig}
import org.peelframework.core.graph.Node
import org.peelframework.core.util.{Version, shell}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

/** This class represents a System in the Peel framework.
  *
  * Most Nodes in the peel dependency-graph are Systems. A system can specify it's dependencies which are then set up
  * automatically, according to their Lifespans.
  *
  * @param name The name of this bean. Deafults to the system name (e.g. "Flink")
  * @param version Version of the system (e.g. "7.1")
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
abstract class System(
  val name: String,
  val version: String,
  val lifespan: Lifespan,
  val dependencies: Set[System],
  val mc: Mustache.Compiler) extends Node with Configurable with BeanNameAware {

  import scala.language.postfixOps

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  var isUp = lifespan == Lifespan.PROVIDED

  /** The system configuration resides under `system.\${configKey}`. */
  val configKey = name

  /** The name of this bean. Deafults to the system name. */
  var beanName = name

  /** Creates a complete system installation with updated configuration and starts the system. */
  def setUp() = {
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
      start()

      logger.info(s"System '$toString' is now up and running")
    }
  }

  /** Cleans up and shuts down the system. */
  def tearDown() = {
    if (!isRunning) {
      logger.info(s"System '$toString' is already down")
    } else {
      logger.info(s"Tearing down system '$toString'")

      stop()
      awaitShutdown()
    }
  }

  /** Restarts the system if the system configuration has changed. */
  def update() = {
    val c = configuration()
    if (!c.hasChanged) {
      logger.info(s"System configuration of '$toString' did not change")
    } else {
      logger.info(s"System configuration of '$toString' changed, restarting...")

      tearDown()
      setUp()

    }
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
  private def materializeHome() = {
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