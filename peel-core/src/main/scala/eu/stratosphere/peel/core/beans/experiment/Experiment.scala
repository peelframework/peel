package eu.stratosphere.peel.core.beans.experiment

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.DataSet
import eu.stratosphere.peel.core.beans.system.ExperimentRunner
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

class Experiment(val runs: Int, val runner: ExperimentRunner, val data: Set[DataSet], val command: String, var config: Config) extends Node {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def run() = {
    logger.info("Running experiment %s".format(config.getString("experiment.name.run")))
    //
    //    for (num <- 0 to sequence.length - 1) {
    //      logger.info("Starting element %d/%d with %d repetitions...".format(num + 1, sequence.length, sequence(num).toInt))
    //
    //      val job: String = arguments(1)
    //      val output = new File(config.getString("path.hadoop.v1.output"), "output.txt")
    //      val results = config.getString("path.hadoop.v1.results")
    //
    //      val fs: List[FileSystem] = getFileSystems(ctx)
    //      def copyInput: List[File] = fs map { x: FileSystem => x.setInput(new File(arguments(2)))}
    //      def copyResults(run: Int) = fs map { x: FileSystem => x.getOutput(output, new File(results, "results_SeqRun_%d_run_%d".format(num + 1, run)))}
    //
    //      //TODO check arguments for correctness
    //
    //      for (run <- 1 to sequence(num)) {
    //        // repeat experiment with sequence number num
    //        logger.info("Running repetition %d/%d".format(run, sequence(num).toInt))
    //        try {
    //          setUp(ctx) // setup runs on reversed graph
    //          runner.run(job, copyInput, output)
    //          copyResults(run)
    //        } catch {
    //          case e: Exception => logger.info("Exception in Experiment run %d of sequence %d: %s".format(run, num, e.getMessage))
    //        } finally {
    //          tearDown(ctx)
    //        }
    //      }
    //
    //      //TODO find good solution to save results of a run
    //    }
  }

  //
  //  def setUp(ctx: ExecutionContext) = {
  //    for (d <- ctx.depGraph.reverse.directDependencies(runner)) {
  //      d match {
  //        case s: System => if (s.lifespan == Lifespan.EXPERIMENT) s.setUp(ctx)
  //        case _ => Unit
  //      }
  //    }
  //  }
  //
  //  def tearDown(ctx: ExecutionContext) = {
  //    for (d <- ctx.depGraph.directDependencies(runner)) {
  //      d match {
  //        case d: System => if (d.lifespan == Lifespan.EXPERIMENT) d.tearDown(ctx)
  //        case _ => Unit
  //      }
  //    }
  //  }
  //
  //  //TODO make filesystem a component of experiment?
  //  def getFileSystems(ctx: ExecutionContext): List[FileSystem] = {
  //    ctx.depGraph.directDependencies(this) flatMap {
  //      case fs: FileSystem => Some(fs)
  //      case _ => None
  //    }
  //  }
  //
  //  def update() = {
  //    // TODO find components with lifespan experiment sequence
  //    // update all of their dependend components but don't update them twice
  //    // => build update graph for every exp_seq component, join graphs
  //  }

  override def toString = "Experiment"
}
