package eu.stratosphere.peel.core.beans.data

import com.typesafe.config.ConfigFactory
import eu.stratosphere.peel.core.beans.system.{FileSystem, System}
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

class ExperimentOutput(val path: String, val fs: System with FileSystem) extends Node with Configurable {

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  /**
   * Clean the data if it exists.
   */
  def clean() = {
    // resolve path from the current config
    val path = resolve(this.path)

    if (fs.exists(path)) {
      logger.info(s"Removing experiment output '$path'")
      if (fs.rmr(path) != 0) throw new RuntimeException(s"Could not remove '$path'")
    }
  }

  /**
   * Alias of name.
   *
   * @return
   */
  override def toString: String = resolve(path)
}
