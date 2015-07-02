package eu.stratosphere.peel.core.beans.data

import com.typesafe.config.ConfigFactory
import eu.stratosphere.peel.core.beans.system.{FileSystem, System}
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

/** Represents an abstract Dataset that can be used in experiments
  *
  * This bean is an abstract representation of a dataset and implements the functionality to
  * physically materialize the data that it represents.
  *
  * @param path Path/location where the data is physically materialized
  * @param dependencies System-Dependencies needed to materialize the dataset (Data-Generators, Systems, ...)
  * */
abstract class DataSet(val path: String, val dependencies: Set[System]) extends Node with Configurable {

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  /** The underlying FileSystem.
    */
  val fs: System with FileSystem

  /** Create the data set represented by this bean.
    */
  def materialize(): Unit

  /** Alias of name.
    */
  override def toString: String = resolve(path)
}
