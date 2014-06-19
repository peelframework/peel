package eu.stratosphere.fab.core.beans.system

import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.Node
import org.slf4j.LoggerFactory
import com.typesafe.config.{ConfigFactory, ConfigException, Config}



/**
 * Created by felix on 09.06.14.
 */
abstract class System(val lifespan: Lifespan, val dependencies: Set[System]) extends Node{

  final val logger = LoggerFactory.getLogger(this.getClass)
  final val config: Config = ConfigFactory.load()
  // TODO add fallback for own configs or merge configs

  def setUp(): Unit

  def tearDown(): Unit

  def update(): Unit

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

}
