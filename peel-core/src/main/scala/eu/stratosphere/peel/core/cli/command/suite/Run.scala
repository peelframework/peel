package eu.stratosphere.peel.core.cli.command.suite

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.experiment.{Experiment, ExperimentSuite}
import eu.stratosphere.peel.core.beans.system.{Lifespan, System}
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.{Configurable, loadConfig}
import eu.stratosphere.peel.core.graph.{Node, createGraph}
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext

class Run extends Command {

  override def name() = "suite:run"

  override def help() = "execute all experiments in a suite"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--experiments")
      .`type`(classOf[String])
      .dest("app.path.experiments")
      .metavar("EXPFILE")
      .help("experiments file (default: config/experiments.xml)")
    parser.addArgument("--force", "-f")
      .`type`(classOf[Boolean])
      .dest("app.suite.experiment.force")
      .action(Arguments.storeTrue)
      .help("re-execute successful runs")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite to run")

    // option defaults
    parser.setDefault("app.path.experiments", "config/experiments.xml")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.path.experiments", Paths.get(ns.getString("app.path.experiments")).normalize.toAbsolutePath.toString)
    Sys.setProperty("app.suite.experiment.force", if (ns.getBoolean("app.suite.experiment.force")) "true" else "false")
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Running experiments in suite '${Sys.getProperty("app.suite.name")}'")

    val suite = context.getBean(Sys.getProperty("app.suite.name"), classOf[ExperimentSuite])
    val graph = createGraph(suite)

    //TODO check for cycles in the graph
    if (graph.isEmpty) throw new RuntimeException("Experiment suite is empty!")

    // resolve experiment configurations
    for (e <- suite.experiments) e.config = loadConfig(graph, e)

    // generate runs to be executed
    val force = Sys.getProperty("app.suite.experiment.force", "false") == "true"
    val runs = for (e <- suite.experiments; i <- 1 to e.runs; r <- Some(e.run(i, force)); if force || !r.isSuccessful) yield r
    // filter experiments in the relevant runs
    val exps = runs.foldRight(List[Experiment[System]]())((r, es) => if (es.isEmpty || es.head != r.exp) r.exp :: es else es)

    // SUITE lifespan
    try {
      logger.info("Executing experiments in suite")
      for (exp <- exps) {

        val allSystems = for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) yield n
        val inpSystems: Set[Node] = for (in <- exp.inputs; sys <- in.dependencies) yield sys
        val expSystems = (graph.descendants(exp, exp.inputs) diff Seq(exp)).toSet

        // EXPERIMENT lifespan
        try {
          logger.info("#" * 60)
          logger.info("Current experiment is '%s'".format(exp.name))

          // update config
          for (n <- graph.descendants(exp)) n match {
            case s: Configurable => s.config = exp.config
            case _ => Unit
          }

          logger.info("Setting up systems required for input data sets")
          for (n <- inpSystems) n match {
            case s: System => s.setUp()
            case _ => Unit
          }

          logger.info("Materializing experiment input data sets")
          for (n <- exp.inputs; path = n.resolve(n.path)) if (!n.fs.exists(path)) {
            try {
              n.materialize()
            } catch {
              case e: Throwable => n.fs.rmr(path); throw e // make sure the path is cleaned for the next try
            }
          } else {
            logger.info(s"Skipping already materialized path '$path}'")
          }

          logger.info("Tearing down redundant systems before conducting experiment runs")
          for (n <- inpSystems diff expSystems) n match {
            case s: System if !(Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan) => s.tearDown()
            case _ => Unit
          }

          logger.info("Setting up systems with SUITE lifespan")
          for (n <- allSystems) n match {
            case s: System if (Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan) && !s.isUp => s.setUp()
            case _ => Unit
          }

          logger.info("Updating systems with PROVIDED or SUITE lifespan")
          for (n <- allSystems) n match {
            case s: System if Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan => s.update()
            case _ => Unit
          }

          logger.info("Setting up systems with EXPERIMENT lifespan")
          for (n <- expSystems) n match {
            case s: System if s.lifespan == Lifespan.EXPERIMENT => s.setUp()
            case _ => Unit
          }

          for (r <- runs if r.exp == exp) {
            for (n <- exp.outputs) n.clean()

            logger.info("Setting up systems with JOB lifespan")
            for (n <- allSystems) n match {
              case s: System if s.lifespan == Lifespan.JOB => s.setUp()
              case _ => Unit
            }

            try {
              r.execute() // run experiment
            }
            finally {
              logger.info("Tearing down systems with JOB lifespan")
              for (n <- allSystems) n match {
                case s: System if s.lifespan == Lifespan.JOB => s.tearDown()
                case _ => Unit
              }
            }

            for (n <- exp.outputs) n.clean()
          }

        } catch {
          case e: Throwable =>
            logger.error(s"Exception for experiment '${exp.name}' in suite '${suite.name}': ${e.getMessage}")
            throw e

        } finally {
          logger.info("Tearing down systems with EXPERIMENT lifespan")
          for (n <- expSystems) n match {
            case s: System if s.lifespan == Lifespan.EXPERIMENT => s.tearDown()
            case _ => Unit
          }
        }
      }

    }
    catch {
      case e: Throwable =>
        logger.error(s"Exception in suite '${suite.name}': ${e.getMessage}")
        throw e

    } finally {
      logger.info("#" * 60)
      logger.info("Tearing down systems with SUITE lifespan")
      for (n <- graph.traverse()) n match {
        case s: System if s.lifespan == Lifespan.SUITE => s.tearDown()
        case _ => Unit
      }
    }
  }
}
