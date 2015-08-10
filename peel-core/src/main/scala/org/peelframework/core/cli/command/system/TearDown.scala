package org.peelframework.core.cli.command.system

import java.lang.{System => Sys}

import org.peelframework.core.beans.system.{Lifespan, System}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.{Configurable, loadConfig}
import org.peelframework.core.graph.createGraph
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Tear down a system. */
@Service("sys:teardown")
class TearDown extends Command {

  override val name = "sys:teardown"

  override val help = "tear down a system"

  override def register(parser: Subparser) = {
    // arguments
    parser.addArgument("system")
      .`type`(classOf[String])
      .dest("app.system.id")
      .metavar("SYSTEM")
      .help("system bean ID")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
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
