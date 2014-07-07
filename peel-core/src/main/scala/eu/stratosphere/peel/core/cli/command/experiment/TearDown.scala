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

class TearDown extends Command {

  override def name() = "exp:teardown"

  override def help() = "tear down systems for a specific experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--fixtures")
      .`type`(classOf[String])
      .dest("app.path.fixtures")
      .metavar("FIXTURES")
      .help("fixtures file")
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
    val exps = suite.experiments.filter(_.name == expName)
    // load config
    for (e <- suite.experiments.filter(_.name == expName)) e.config = loadConfig(graph, e)
    // check if experiment exists (the list should contain exactly one element)
    if (exps.size != 1) throw new RuntimeException(s"Experiment '$expName' either not found or ambigous in suite '$suiteName'")

    for (exp <- exps) {
      // update config
      for (n <- graph.descendants(exp)) n match {
        case s: Configurable => s.config = exp.config
        case _ => Unit
      }

      logger.info("Tearing down systems for experiment '%s'".format(exp.name))
      for (n <- graph.traverse(); if graph.descendants(exp).contains(n)) n match {
        case s: System => if (Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan) {
          s.tearDown()
        }
        case _ => Unit
      }
    }

  }
}
