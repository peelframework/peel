package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.ExecutionContext
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import java.io.File
import eu.stratosphere.fab.core.beans.Shell

class Hadoop(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = {

  }

  def tearDown(): Unit = {

  }

  def update(): Unit = {

  }

  def run(job: String) = {
    logger.info("Running Hadoop Job %s ...".format(job))
    val home: String = config.getString("paths.hadoop.v1.home")
    val input = config.getString("paths.hadoop.v1.input")
    val output = config.getString("paths.hadoop.v1.output")
    Shell.execute(home + "/bin/hadoop jar %s %s %s".format(job, input, output), true)
  }

  override def toString: String = "Hadoop v1"
}
