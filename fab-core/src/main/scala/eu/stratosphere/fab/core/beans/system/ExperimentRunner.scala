package eu.stratosphere.fab.core.beans.system

import eu.stratosphere.fab.core.beans.ExecutionContext
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan


/**
 * Created by felix on 02.06.14.
 */
abstract class ExperimentRunner(name: String, lifespan: Lifespan, dependencies: java.util.Set[System])
  extends System(name, lifespan, dependencies) {


  def run(context: ExecutionContext) = ???

  override def toString() = {
    "ExperimentRunner " + name
  }

}
