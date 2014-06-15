package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.ExecutionContext

class Hadoop(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = {
    logger.info("Setting up Hadoop...")
  }

  override def run(ctx: ExecutionContext) = {
    logger.info("Running Hadoop Job...")
  }

  def tearDown(): Unit = {
    logger.info("Tearing down Hadoop...")
  }

  override def toString: String = "Hadoop v1"
}
