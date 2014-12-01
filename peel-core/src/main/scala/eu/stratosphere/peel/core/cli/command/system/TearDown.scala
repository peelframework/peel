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

  override def name() = "sys:teardown"

  override def help() = "tear down a system"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--fixtures")
      .`type`(classOf[String])
      .dest("app.path.fixtures")
      .metavar("FIXTURES")
      .help("fixtures file")
    // arguments
    parser.addArgument("system")
      .`type`(classOf[String])
      .dest("app.system.name")
      .metavar("SYSTEM")
      .help("system bean ID")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    if (ns.getString("app.path.fixtures") != null) Sys.setProperty("app.path.fixtures", Paths.get(ns.getString("app.path.fixtures")).normalize.toAbsolutePath.toString)
    Sys.setProperty("app.system.name", ns.getString("app.system.name"))
  }

  override def run(context: ApplicationContext) = {
    val systemName = Sys.getProperty("app.system.name")

    logger.info(s"Tearing down system '$systemName' and dependencies with SUITE or EXPERIMENT lifespan")
    val sys = context.getBean(systemName, classOf[System])
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
