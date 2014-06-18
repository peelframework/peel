package eu.stratosphere.fab.core

import eu.stratosphere.fab.core.beans.system.System
import com.typesafe.config.{ConfigException, Config}
import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._


/**
 * Created by felix on 18.06.14.
 *
 * represents a map from all optional parameters to the systems that are influenced by these parameters
 */
object Options {
  val options = HashMap("nodes" -> "hdfs")

  def getSystems(option: String): String = options.get(option) match {
    case Some(o) => o
    case None => throw new RuntimeException("Option " + option + " is not defined!")
  }


  //def parseOptions(conf: Config) = ???
  //TODO parse options with (optional) parameters
}
