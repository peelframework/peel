package eu.stratosphere.peel.core.beans.system

import java.nio.file.{Paths, Files}

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigFactory
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.config.{Configurable, SystemConfig}
import eu.stratosphere.peel.core.graph.Node
import eu.stratosphere.peel.core.util.shell
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

abstract class System(val defaultName: String,
                      val lifespan: Lifespan,
                      val dependencies: Set[System],
                      val mc: Mustache.Compiler) extends Node with Configurable with BeanNameAware {

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  var isUp = lifespan == Lifespan.PROVIDED

  /**
   * The name of this bean. Deafults to the system name.
   */
  var name = defaultName

  /**
   * Creates a complete system installation with updated configuration and starts the system.
   */
  def setUp() = {
    if (isUp) {
      logger.info(s"System '$toString' is already up and running")
    } else {
      logger.info(s"Starting system '$toString'")

      if (config.hasPath(s"system.$defaultName.path.archive")) {
        if (!Files.exists(Paths.get(config.getString(s"system.$defaultName.path.home")))) {
          logger.info(s"Extracting archive ${config.getString(s"system.$defaultName.path.archive.src")} to ${config.getString(s"system.$defaultName.path.archive.dst")}")
          shell.untar(config.getString(s"system.$defaultName.path.archive.src"), config.getString(s"system.$defaultName.path.archive.dst"))

          logger.info(s"Changing owner of ${config.getString(s"system.$defaultName.path.home")} to ${config.getString(s"system.$defaultName.user")}:${config.getString(s"system.$defaultName.group")}")
          shell ! "chown -R %s:%s %s".format(
            config.getString(s"system.$defaultName.user"),
            config.getString(s"system.$defaultName.group"),
            config.getString(s"system.$defaultName.path.home"))
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
    if (!isUp) {
      logger.info(s"System '$toString' is already down")
    } else {
      logger.info(s"Tearing down system '$toString'")

      stop()
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

      stop()
      c.update()
      start()

      logger.info(s"System '$toString' is now running")
    }
  }

  /**
   * Bean name setter.
   *
   * @param n The configured bean name
   */
  override def setBeanName(n: String) = name = n

  /**
   * Alias of name.
   *
   * @return
   */
  override def toString: String = name

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
}

object System {

  case object State extends Enumeration {
    type State = Value
    final val RUNNING, STOPPED = Value
  }

}