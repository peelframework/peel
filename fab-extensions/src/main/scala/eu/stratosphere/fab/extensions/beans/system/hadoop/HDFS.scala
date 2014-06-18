package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.System
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set()) extends System(lifespan, dependencies) {

  var iteration = 0

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
  }

  def update(): Unit = {
    logger.info("Updating " + toString + "...")
    tearDown()
    setUp()
  }

  override def toString = "HDFS v1"

}
