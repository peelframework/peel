package eu.stratosphere.peel.core.cli.command.experiment

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.experiment.ExperimentSuite
import eu.stratosphere.peel.core.beans.system.{Lifespan, System}
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.{Configurable, loadConfig}
import eu.stratosphere.peel.core.graph.{Node, createGraph}
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext

/** Command that is used to set up the specified system from the fixture
  *
  */
class SetUp extends Command {

  override def name() = "exp:setup"

  override def help() = "set up systems for a specific experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--experiments")
      .`type`(classOf[String])
      .dest("app.path.experiments")
      .metavar("EXPFILE")
      .help("experiments file (default: config/experiments.xml)")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("suite containing the experiment")
    parser.addArgument("experiment")
      .`type`(classOf[String])
      .dest("app.suite.experiment.name")
      .metavar("EXPERIMENT")
      .help("experiment to run")

    // option defaults
    parser.setDefault("app.path.experiments", "config/experiments.xml")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.path.experiments", Paths.get(ns.getString("app.path.experiments")).normalize.toAbsolutePath.toString)
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.suite.experiment.name", ns.getString("app.suite.experiment.name"))
  }

  override def run(context: ApplicationContext) = {
    val suiteName = Sys.getProperty("app.suite.suite.name")
    val expName = Sys.getProperty("app.suite.experiment.name")

    logger.info(s"Running experiment '${Sys.getProperty("app.suite.experiment.name")}' from suite '${Sys.getProperty("app.suite.name")}'")
    val suite = context.getBean(Sys.getProperty("app.suite.name"), classOf[ExperimentSuite])
    val graph = createGraph(suite)

    //TODO check for cycles in the graph
    if (graph.isEmpty) throw new RuntimeException("Experiment suite is empty!")

    // find experiment
    val expOption = suite.experiments.find(_.name == expName)
    // check if experiment exists (the list should contain exactly one element)
    if (expOption.isEmpty) throw new RuntimeException(s"Experiment '$expName' either not found or ambigous in suite '$suiteName'")

    for (exp <- expOption) {

      val allSystems = for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) yield n
      val inpSystems: Set[Node] = for (in <- exp.inputs; sys <- in.dependencies) yield sys
      val expSystems = (graph.descendants(exp, exp.inputs) diff Seq(exp)).toSet

      try {
        // update config
        exp.config = loadConfig(graph, exp)
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

        logger.info("Setting up systems with EXPERIMENT lifespan")
        for (n <- expSystems) n match {
          case s: System if Lifespan.EXPERIMENT :: Nil contains s.lifespan => s.setUp()
          case _ => Unit
        }

      } catch {
        case e: Throwable =>
          logger.error(s"Exception of type ${e.getClass.getCanonicalName} for experiment ${exp.name} in suite ${suite.name}: ${e.getMessage}")

          logger.info("Tearing down running systems with SUITE or EXPERIMENT lifespan")
          for (n <- allSystems) n match {
            case s: System => if ((Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan) && s.isUp) s.tearDown()
            case _ => Unit
          }

          throw e
      }
    }

  }
}
