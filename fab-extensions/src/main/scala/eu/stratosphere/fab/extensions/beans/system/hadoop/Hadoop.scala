package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.ExecutionContext
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

class Hadoop(name: String, lifespan: Lifespan, dependencies: java.util.Set[System] = new java.util.HashSet()) extends ExperimentRunner(name, lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

  override def run(context: ExecutionContext) = ???

}
