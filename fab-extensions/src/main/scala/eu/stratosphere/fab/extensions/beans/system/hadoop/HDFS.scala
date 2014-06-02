package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.System

class HDFS(name: String, lifespan: System.Lifespan, dependencies: java.util.Set[System] = new java.util.HashSet()) extends System(name, lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

}
