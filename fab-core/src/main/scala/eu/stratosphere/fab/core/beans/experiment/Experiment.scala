package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.system.{Lifespan, ExperimentRunner, System}
import eu.stratosphere.fab.core.{ExecutionContext, Node}
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory

/**
 * Created by felix on 12.06.14.
 */
class Experiment (val runner: ExperimentRunner,
                  val arguments: List[String] = List())
                  extends Node {

  final val logger = LoggerFactory.getLogger(this.getClass)
  final val conf = ConfigFactory.load(arguments(2))


  def run(ctx: ExecutionContext) = {
    // set up the runner and every system with lifespan experiment or experiment sequence
    logger.info("Starting Experiment with " + ctx.runs + " repetitions")

    for (run <- 1 to ctx.runs) {
      logger.info("Running repetition " + run + " of " + ctx.runs)
      setUpComponents(ctx.expGraph.reverse.directDependencies(runner)) // setup runs on reversed graph
      runner.run(ctx)
      tearDownComponents(ctx.expGraph.directDependencies(runner))
    }
    //TODO find good solution to save results of a run
  }

  def setUpComponents(systems: List[Node]) = {
    for(s <- systems ) yield {
      s match {
      case s: System => if (s.lifespan == Lifespan.EXPERIMENT) s.setUp()
      case x => x
      }
    }
  }

  def tearDownComponents(systems: List[Node]) = {
    for(s <- systems ) yield {
      s match {
        case s: System => if (s.lifespan == Lifespan.EXPERIMENT) s.tearDown()
        case x => x
      }
    }
  }

  def update() = {
    // TODO find components with lifespan experiment sequence
    // update all of their dependend components but don't update them twice
    // => build update graph for every exp_seq component, join graphs
  }


}
