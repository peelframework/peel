package eu.stratosphere.peel.core.beans.system

import com.samskivert.mustache.Mustache
import com.typesafe.config.{ConfigFactory, Config}
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.config.{Configurable, SystemConfig}
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

abstract class System(val defaultName: String,
                      val lifespan: Lifespan,
                      val dependencies: Set[System],
                      val mc: Mustache.Compiler) extends Node with Configurable with BeanNameAware {

  import scala.language.implicitConversions

  final val logger = LoggerFactory.getLogger(this.getClass)

  final val pollingInterval = 3000
  final val pollingCounter = 20

  override var config = ConfigFactory.empty()

  /**
   * The name of this bean. Deafults to the system name.
   */
  var name = defaultName

  /**
   * Creates a complete system installation with updated configuration and starts the system.
   */
  def setUp(): Unit

  /**
   * Cleans up and shuts down the system.
   */
  def tearDown(): Unit

  /**
   * Restarts the system if the system configuration has changed.
   */
  def update(): Unit

  /**
   * Returns an of the system configuration using the current Config
   */
  def configuration(): SystemConfig

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

  // TODO: move to a util class or Shell
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}

object System {
}
