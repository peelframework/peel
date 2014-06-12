package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.system.ExperimentRunner
import eu.stratosphere.fab.core.{ExecutionContext, Node}
import scala.collection.immutable.HashMap

/**
 * Created by felix on 12.06.14.
 */
class Experiment (val runner: ExperimentRunner,
                  val arguments: List[String] = List())  extends Node{

  def run(ctx: ExecutionContext) = ???

}
