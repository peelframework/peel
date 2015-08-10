package org.peelframework.core.beans.data

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.system.{FileSystem, System}
import org.peelframework.core.config.Configurable
import org.peelframework.core.graph.Node
import org.slf4j.LoggerFactory

/** Represents the output of an experiment
 *
 * @param path The [[org.peelframework.core.beans.system.FileSystem FileSystem]] path for the output.
 * @param fs The [[org.peelframework.core.beans.system.FileSystem FileSystem]] containing the output.
 */
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
