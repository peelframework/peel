package eu.stratosphere.peel.core.beans.system

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigFactory
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.config.{Configurable, SystemConfig}
import eu.stratosphere.peel.core.graph.Node
import eu.stratosphere.peel.core.util.shell
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

abstract class System(val name: String,
                      val version: String,
                      val lifespan: Lifespan,
                      val dependencies: Set[System],
                      val mc: Mustache.Compiler) extends Node with Configurable with BeanNameAware {

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  var isUp = lifespan == Lifespan.PROVIDED

  /**
   * The system configuration resides under `system.${configKey}`.
   */
  val configKey = name

  /**
   * The name of this bean. Deafults to the system name.
   */
  var beanName = name

  /**
   * Creates a complete system installation with updated configuration and starts the system.
   */
  def setUp() = {
    if (isRunning) {
      if (isUp)
        logger.info(s"System '$toString' is already up and running")
      else
        logger.warn(s"System '$toString' is running but not up. Shut down manually '$toString' or set its bean lifecycle to PROVIDED.")
    } else {
      logger.info(s"Starting system '$toString'")

      if (!Files.exists(Paths.get(config.getString(s"system.$configKey.path.home")))) {
        if (config.hasPath(s"system.$configKey.path.archive")) {
          logger.info(s"Extracting archive ${config.getString(s"system.$configKey.path.archive.src")} to ${config.getString(s"system.$configKey.path.archive.dst")}")
          shell.extract(config.getString(s"system.$configKey.path.archive.src"), config.getString(s"system.$configKey.path.archive.dst"))

          logger.info(s"Changing owner of ${config.getString(s"system.$configKey.path.home")} to ${config.getString(s"system.$configKey.user")}:${config.getString(s"system.$configKey.group")}")
          shell ! "chown -R %s:%s %s".format(
            config.getString(s"system.$configKey.user"),
            config.getString(s"system.$configKey.group"),
            config.getString(s"system.$configKey.path.home"))
        } else {
          throw new RuntimeException(s"Cannot find archive path for system '$configKey'")
        }
      }

      configuration().update()
      start()

      logger.info(s"System '$toString' is now up and running")
    }
  }

  /**
   * Cleans up and shuts down the system.
   */
  def tearDown() = {
    if (!isRunning) {
      logger.info(s"System '$toString' is already down")
    } else {
      logger.info(s"Tearing down system '$toString'")

      stop()
      awaitShutdown()
    }
  }

  /**
   * Restarts the system if the system configuration has changed.
   */
  def update() = {
    val c = configuration()
    if (!c.hasChanged) {
      logger.info(s"System configuration of '$toString' did not change")
    } else {
      logger.info(s"System configuration of '$toString' changed, restarting...")

      tearDown()
      setUp()
      start()

      logger.info(s"System '$toString' is now running")
    }
  }

  private def awaitShutdown() : Unit = {
    var maxAttempts = config.getInt("system.default.stop.max.attempts")
    while (isRunning) {
      // wait a bit
      Thread.sleep(config.getInt("system.default.stop.polling.interval"))
      if (maxAttempts <= 0) {
        throw new RuntimeException(s"Unable to shut down system '$toString' in time.")
      }
      maxAttempts = maxAttempts - 1
    }
    logger.info(s"Shut down system '$toString'.")
  }

  /**
   * Bean name setter.
   *
   * @param n The configured bean name
   */
  override def setBeanName(n: String) = beanName = n

  /**
   * Alias of name.
   *
   * @return
   */
  override def toString: String = beanName

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  /**
   * Returns an of the system configuration using the current Config
   */
  protected def configuration(): SystemConfig

  /**
   * Starts up the system and polls to check whether everything is up.
   *
   * @throws SetUpTimeoutException If the system was not brought after {startup.pollingCounter} times {startup.pollingInterval} milliseconds.
   */
  protected def start(): Unit

  /**
   * Stops the system.
   */
  protected def stop(): Unit

  /**
   * Checks whether a process for this system is already running.
   *
   * This is different from the value of `isUp`, as a system can be running, but not yet up and operational (i.e. if
   * not all worker nodes of a distributed have connected).
   *
   * @return True if a system process for this system exists.
   */
  def isRunning: Boolean

  /**
   * Returns the template path closest to the given system and version.
   */
  protected def templatePath(path: String) = {
    // initialize version and template path
    var v = version.stripSuffix("-SNAPSHOT")
    var z = Option(this.getClass.getResource(s"/templates/$configKey/$v/$path.mustache"))
    // iterate while parent version exists and current version does not have a specific template
    while (!v.isEmpty && z.isEmpty) {
      v = v.substring(0, Math.max(0, v.lastIndexOf('.')))
      z = Option(this.getClass.getResource(s"/templates/$configKey/$v/$path.mustache"))
    }
    // if version template exists return its path, otherwise return the base tempalte path
    if (v.isEmpty)
      s"/templates/$configKey/$path.mustache"
    else
      s"/templates/$configKey/$v/$path.mustache"
  }
}