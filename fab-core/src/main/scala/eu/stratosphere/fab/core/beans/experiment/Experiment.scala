package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.system.{FileSystem, Lifespan, ExperimentRunner, System}
import eu.stratosphere.fab.core.{ExecutionContext, Node}
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import java.io.{FileNotFoundException, File}
import java.nio.file.FileSystemNotFoundException

/**
 * Created by felix on 12.06.14.
 */
class Experiment (val runner: ExperimentRunner,
                  val arguments: List[String] = List())
                  extends Node {

  final val logger = LoggerFactory.getLogger(this.getClass)
  final val conf = ConfigFactory.load(arguments(0))


  def run(ctx: ExecutionContext) = {
    // set up the runner and every system with lifespan experiment or experiment sequence
    logger.info("Starting Experiment with " + ctx.runs + " repetitions")
    val job: String = arguments(1)
    val input: File = new File(arguments(2))
    val output: File = new File(arguments(3))

    for (run <- 1 to ctx.runs) {
      logger.info("Running repetition " + run + " of " + ctx.runs)
      setUpComponents(ctx.expGraph.reverse.directDependencies(runner)) // setup runs on reversed graph
      copyDataToFS(ctx.expGraph.directDependencies(runner), input)
      runner.run(job)
      tearDownComponents(ctx.expGraph.directDependencies(runner))
    }
    //TODO find good solution to save results of a run
  }

  def copyDataToFS(systems: List[Node], input: File) {
    val fileSystems: List[FileSystem] = systems flatMap { case fs: FileSystem => Some(fs)
    case _ => None}
    fileSystems.head.setInput(input)
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
