package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.system.{FileSystem, Lifespan, ExperimentRunner, System}
import eu.stratosphere.fab.core.{ExecutionContext, Node}
import org.slf4j.LoggerFactory
import com.typesafe.config.ConfigFactory
import java.io.{FileNotFoundException, File}
import scala.collection.JavaConverters._

/**
 * Created by felix on 12.06.14.
 */
class Experiment (val runner: ExperimentRunner,
                  val arguments: List[String] = List())
                  extends Node {

  final val logger = LoggerFactory.getLogger(this.getClass)
  final val ExpConf = ConfigFactory.load(arguments(0))
  final val config = ConfigFactory.load()
  final val sequence = ExpConf.getIntList("experiment.runs").asScala.toList


  def run(ctx: ExecutionContext) = {
    logger.info("Starting experiment sequence with %d element(s)...".format(sequence.length))
    for (num <- 0 to sequence.length - 1) { // start experiment sequence
      logger.info("Starting element %d/%d with %d repetitions...".format(num+1, sequence.length, sequence(num).toInt))
      val job: String = arguments(1)
      val output = new File(config.getString("paths.hadoop.v1.output"), "output.txt")
      val results = config.getString("paths.hadoop.v1.results")

      val fs: List[FileSystem] = getFileSystems(ctx)
      def copyInput: List[File] = fs map { x: FileSystem => x.setInput(new File(arguments(2)))}
      def copyResults(run: Int) = fs map { x: FileSystem => x.getOutput(output, new File(results, "results_SeqRun_%d_run_%d".format(num+1, run)))}

      //TODO check arguments for correctness

      for (run <- 1 to sequence(num)) { // repeat experiment with sequence number num
        logger.info("Running repetition %d/%d".format(run, sequence(num).toInt))
        setUpComponents(ctx.expGraph.reverse.directDependencies(runner)) // setup runs on reversed graph
        runner.run(job, copyInput, output)
        copyResults(run)
        tearDownComponents(ctx.expGraph.directDependencies(runner))
      }
      //TODO find good solution to save results of a run
    }
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

  //TODO make filesystem a component of experiment?
  def getFileSystems(ctx: ExecutionContext): List[FileSystem] = {
    ctx.expGraph.directDependencies(this) flatMap {
      case fs: FileSystem => Some(fs)
      case _ => None
    }
  }

  def update() = {
    // TODO find components with lifespan experiment sequence
    // update all of their dependend components but don't update them twice
    // => build update graph for every exp_seq component, join graphs
  }


}
