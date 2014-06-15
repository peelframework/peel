package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.system.{Lifespan, ExperimentRunner, System}
import eu.stratosphere.fab.core.{ExecutionContext, Node}
import scala.collection.immutable.HashMap
import org.slf4j.LoggerFactory

/**
 * Created by felix on 12.06.14.
 */
class Experiment (val runner: ExperimentRunner,
                  val arguments: List[String] = List())
                  extends Node {
  final val logger = LoggerFactory.getLogger(this.getClass)

  def run(ctx: ExecutionContext) = {
    // set up the runner and every system with lifespan experiment or experiment sequence
    setUpComponents(ctx.expGraph.directDependencies(runner))
    runner.run(ctx)
    tearDownComponents(ctx.expGraph.directDependencies(runner))
  }

  //TODO setup should run in reverse order of the dependencies

  //TODO find solution for experiment and experiment_sequence systems

  def setUpComponents(systems: List[Node]) = {
    for(s <- systems ) yield {
      s match {
      case s: System => if (s.lifespan == Lifespan.EXP_SEQ | s.lifespan == Lifespan.EXPERIMENT) s.setUp()
      case x => x
      }
    }
  }

  def tearDownComponents(systems: List[Node]) = {
    for(s <- systems ) yield {
      s match {
        case s: System => if (s.lifespan == Lifespan.EXP_SEQ | s.lifespan == Lifespan.EXPERIMENT) s.tearDown()
        case x => x
      }
    }
  }


}
