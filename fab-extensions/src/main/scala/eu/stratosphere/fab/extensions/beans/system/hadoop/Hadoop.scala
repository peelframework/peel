package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.ExecutionContext

class Hadoop(name: String, lifespan: System.Lifespan, dependencies: java.util.Set[System] = new java.util.HashSet()) extends ExperimentRunner(name, lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

  override def run(context: ExecutionContext) = ???

}
