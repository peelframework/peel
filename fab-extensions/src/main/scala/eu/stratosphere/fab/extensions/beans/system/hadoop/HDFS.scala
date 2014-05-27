package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.System

class HDFS(lifespan: System.Lifespan, dependencies: java.util.Set[System] = new java.util.HashSet()) extends System(lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

}
