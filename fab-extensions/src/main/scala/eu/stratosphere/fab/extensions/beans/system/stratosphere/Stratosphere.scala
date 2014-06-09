package eu.stratosphere.fab.extensions.beans.system.stratosphere

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.ExecutionContext
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

/**
 * Created by felix on 01.06.14.
 */
class Stratosphere(name: String, lifespan: Lifespan, dependencies: java.util.Set[System] = new java.util.HashSet()) extends ExperimentRunner(name, lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

  override def run(context: ExecutionContext) = ???
}
