package eu.stratosphere.peel.core.cli.command.experiment

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.experiment.ExperimentSuite
import eu.stratosphere.peel.core.beans.system.{Lifespan, System}
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.{Configurable, loadConfig}
import eu.stratosphere.peel.core.graph.createGraph
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext

class SetUp extends Command {

  override def name() = "exp:setup"

  override def help() = "set up systems for a specific experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--fixtures")
      .`type`(classOf[String])
      .dest("app.path.fixtures")
      .metavar("FIXTURES")
      .help("fixtures file (default: config/fixtures.xml)")
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
    parser.setDefault("app.path.fixtures", "config/fixtures.xml")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.path.fixtures", Paths.get(ns.getString("app.path.fixtures")).normalize.toAbsolutePath.toString)
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
      try {
        // update config
        exp.config = loadConfig(graph, exp)
        for (n <- graph.descendants(exp)) n match {
          case s: Configurable => s.config = exp.config
          case _ => Unit
        }

        logger.info("Setting up systems with SUITE or EXPERIMENT lifespan")
        for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) n match {
          case s: System => if ((Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan) && !s.isUp) s.setUp()
          case _ => Unit
        }

        logger.info("Materializing experiment input data sets")
        for (n <- exp.inputs) n.materialize()
      } catch {
        case e: Throwable =>
          logger.error(s"Exception of type ${e.getClass.getCanonicalName} for experiment ${exp.name} in suite ${suite.name}: ${e.getMessage}")

          logger.info("Tearing down runing systems with SUITE or EXPERIMENT lifespan")
          for (n <- graph.traverse(); if graph.descendants(exp).contains(n)) n match {
            case s: System => if ((Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan) && s.isUp) s.tearDown()
            case _ => Unit
          }

          throw e
      }
    }

  }
}
