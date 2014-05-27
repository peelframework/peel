package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.ExecutionContext

class Hadoop(lifespan: System.Lifespan, dependencies: java.util.Set[System] = new java.util.HashSet()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

  def run(context: ExecutionContext): Unit = ???

}
