package eu.stratosphere.peel.core.cli.command.system

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.system.{Lifespan, System}
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.{Configurable, loadConfig}
import eu.stratosphere.peel.core.graph.createGraph
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext

/** Command that is used to set up the dependencies for the experiment
  *
  */
class TearDown extends Command {

  override val name = "sys:teardown"

  override val help = "tear down a system"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--experiments")
      .`type`(classOf[String])
      .dest("app.path.experiments")
      .metavar("EXPFILE")
      .help("experiments file (default: config/experiments.xml)")
    // arguments
    parser.addArgument("system")
      .`type`(classOf[String])
      .dest("app.system.id")
      .metavar("SYSTEM")
      .help("system bean ID")

    // option defaults
    parser.setDefault("app.path.experiments", "config/experiments.xml")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.path.experiments", Paths.get(ns.getString("app.path.experiments")).normalize.toAbsolutePath.toString)
    Sys.setProperty("app.system.id", ns.getString("app.system.id"))
  }

  override def run(context: ApplicationContext) = {
    val systemID = Sys.getProperty("app.system.id")

    logger.info(s"Tearing down system '$systemID' and dependencies with SUITE or EXPERIMENT lifespan")
    val sys = context.getBean(systemID, classOf[System])
    val graph = createGraph(sys)

    // update config
    sys.config = loadConfig(graph, sys)
    for (n <- graph.descendants(sys)) n match {
      case s: Configurable => s.config = sys.config
      case _ => Unit
    }

    // tear down
    for (n <- graph.traverse(); if graph.descendants(sys).contains(n)) n match {
      case s: System if Lifespan.SUITE :: Lifespan.EXPERIMENT :: Nil contains s.lifespan => s.tearDown()
      case _ => Unit
    }
  }
}
