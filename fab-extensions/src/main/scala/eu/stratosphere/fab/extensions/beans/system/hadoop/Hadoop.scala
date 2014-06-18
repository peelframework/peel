package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.ExecutionContext
import com.typesafe.config.Config

class Hadoop(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
  }

  override def run(ctx: ExecutionContext) = {
    logger.info("Running Hadoop Job...")
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
  }

  def update(): Unit = {
    logger.info("Updating " + toString + "...")
  }

  override def toString: String = "Hadoop v1"
}
