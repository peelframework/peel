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

import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.core.beans.system.{Lifespan, System}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.{Configurable, loadConfig}
import org.peelframework.core.graph.createGraph
import org.peelframework.core.util.console._
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Execute a specific experiment. */
@Service("exp:run")
class Run extends Command {

  override val help = "execute a specific experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--just")
      .`type`(classOf[Boolean])
      .dest("app.suite.experiment.just")
      .action(Arguments.storeTrue)
      .help("skip system set-up and tear-down")
    parser.addArgument("--run")
      .`type`(classOf[Integer])
      .dest("app.suite.experiment.run")
      .metavar("RUN")
      .help("run to execute")
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
    parser.setDefault("app.suite.experiment.run", 1)
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.suite.experiment.just", if (ns.getBoolean("app.suite.experiment.just")) "true" else "false")
    Sys.setProperty("app.suite.experiment.run", ns.getInt("app.suite.experiment.run").toString)
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.suite.experiment.name", ns.getString("app.suite.experiment.name"))
  }

  override def run(context: ApplicationContext) = {
    val suiteName = Sys.getProperty("app.suite.name")
    val expName = Sys.getProperty("app.suite.experiment.name")
    val expRun = Sys.getProperty("app.suite.experiment.run").toInt
    val justRun = Sys.getProperty("app.suite.experiment.just") == "true"

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

    for {
      e <- exps
      r <- Some(e.run(expRun, force = true))
    } {
      // traverse all systems relevant for this experiment
      def allSystems(reverse: Boolean) = for {
        System(s) <- if (reverse) graph.reverse.traverse() else graph.traverse()
        if graph.descendants(e).contains(s)
      } yield s

      // traverse all systems relevant for this experiment
      // and required by at least one experiment input
      def inpSystems(reverse: Boolean) = {
        // compute the set of input descendants
        val inpDescendant = for {
          i <- e.inputs
          d <- graph.descendants(i)
        } yield d
        allSystems(reverse = reverse).filter(inpDescendant)
      }

      // traverse all systems relevant for this experiment
      // and not required by any experiment input
      def expSystems(reverse: Boolean) = {
        // compute the set of experiment, non-input descendants
        val expDescendant = graph.descendants(e, e.inputs).toSet
        allSystems(reverse = reverse).filter(expDescendant)
      }

      try {
        // update config
        for (Configurable(c) <- graph.descendants(e))
          c.config = e.config

        if (!justRun) {

          logger.info("Setting up / updating systems required for input data sets")
          for (s <- inpSystems(reverse = true)) {
            if (s.isUp) s.update()
            else s.setUp()
          }

          logger.info("Ensuring that input data sets exist")
          for (i <- e.inputs)
            i.ensureExists()

          logger.info("Tearing down redundant systems before conducting experiment runs")
          for {
            s <- inpSystems(reverse = false)
            isRedundant = !(expSystems(reverse = false) contains s)
            if (isRedundant && s.lifespan < Lifespan.SUITE) || s.lifespan == Lifespan.RUN
          } s.tearDown()

          logger.info("Setting up / updating systems required for experiment")
          for {
            s <- expSystems(reverse = true)
            if s.lifespan > Lifespan.RUN
            if s.lifespan < Lifespan.PROVIDED
          } {
            if (s.isUp) s.update()
            else s.setUp()
          }

        } else {
          logger.info("Updating all systems")
          for {
            System(s) <- allSystems(reverse = true)
          } s.update()
        }

        logger.info("Setting up systems with RUN lifespan")
        for {
          s <- expSystems(reverse = true)
          if s.lifespan == Lifespan.RUN
        } s.setUp()

        logger.info("Cleaning output datasets")
        for (n <- e.outputs)
          n.clean()

        // r experiment
        r.execute()
        // stop for a brief intermission between runs
        Thread.sleep(r.exp.config.getLong("experiment.run.intermission"))

      } catch {
        case t: Throwable =>
          logger.error(s"Exception for experiment ${e.name} in suite ${suite.name}: ${t.getMessage}".red)
          throw t

      } finally {
        logger.info("Tearing down systems with RUN lifespan")
        for {
          s <- allSystems(reverse = false)
          if s.lifespan == Lifespan.RUN
        } s.tearDown()

        if (!justRun) {
          logger.info("Tearing down systems with SUITE or EXPERIMENT lifespan")
          for {
            s <- allSystems(reverse = false)
            if s.lifespan != Lifespan.RUN
            if s.lifespan <= Lifespan.SUITE
          } s.tearDown()
        }
      }
    }

  }
}
