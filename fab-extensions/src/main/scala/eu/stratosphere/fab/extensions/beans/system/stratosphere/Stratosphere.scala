package eu.stratosphere.fab.extensions.beans.system.stratosphere

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.ExecutionContext
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

/**
 * Created by felix on 01.06.14.
 */
class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
  }

  override def run(ctx: ExecutionContext) = {
    logger.info("Running Stratosphere Job...")
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
  }

  def update(): Unit = {
    logger.info("Updating " + toString + "...")
  }

  override def toString = "Stratosphere v0.5"
}
