/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.core.cli.command.experiment

import java.lang.{System => Sys}

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.core.beans.system.{Lifespan, System}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.{Configurable, loadConfig}
import org.peelframework.core.graph.createGraph
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Tear down systems for a specific experiment. */
@Service("exp:teardown")
class TearDown extends Command {

  override val help = "tear down systems for a specific experiment"

  override def register(parser: Subparser) = {
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
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.suite.experiment.name", ns.getString("app.suite.experiment.name"))
  }

  override def run(context: ApplicationContext) = {
    val suiteName = Sys.getProperty("app.suite.name")
    val expName = Sys.getProperty("app.suite.experiment.name")

    logger.info(s"Running experiment '$expName' from suite '$suiteName'")

    val suite = context.getBean(suiteName, classOf[ExperimentSuite])
    val graph = createGraph(suite)

    //TODO check for cycles in the graph
    if (graph.isEmpty)
      throw new RuntimeException("Experiment suite is empty!")

    // find experiment
    val exps = suite.experiments.filter(_.name == expName)

    // check if experiment exists (the list should contain exactly one element)
    if (exps.size != 1)
      throw new RuntimeException(s"Experiment '$expName' either not found or ambiguous in suite '$suiteName'")

    // load config
    for (e <- exps)
      e.config = loadConfig(graph, e)

    for (e <- exps) {
      // traverse all systems relevant for this experiment
      def allSystems(reverse: Boolean) = for {
        System(s) <- if (reverse) graph.reverse.traverse() else graph.traverse()
        if graph.descendants(e).contains(s)
      } yield s

      // update config
      e.config = loadConfig(graph, e)
      for (Configurable(c) <- graph.descendants(e))
        c.config = e.config

      logger.info("Tearing down systems for experiment '%s'".format(e.name))
      for {
        s <- allSystems(reverse = false)
        if s.lifespan <= Lifespan.SUITE
      } s.tearDown()
    }

  }
}
