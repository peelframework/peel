package eu.stratosphere.fab.core.beans.system

import eu.stratosphere.fab.core.beans.ExecutionContext
import scala.collection.JavaConverters._


/**
 * Created by felix on 02.06.14.
 */
abstract class ExperimentRunner(name: String, lifespan: System.Lifespan, dependencies: java.util.Set[System])
  extends System(name, lifespan, dependencies) {

  final val dependencySet: Set[System] = dependencies.asScala.toSet

  def run(context: ExecutionContext) = ???

}
