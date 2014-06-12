package eu.stratosphere.fab.extensions.beans.system.stratosphere

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.ExecutionContext
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

/**
 * Created by felix on 01.06.14.
 */
class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???

  override def run(context: ExecutionContext) = ???
}
