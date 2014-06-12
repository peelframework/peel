package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

class Hadoop(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = ???

  def tearDown(): Unit = ???


}
