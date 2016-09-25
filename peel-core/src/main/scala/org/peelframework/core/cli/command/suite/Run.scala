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
package org.peelframework.core.cli.command.suite

import java.lang.{System => Sys}

import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.beans.experiment.{Experiment, ExperimentSuite}
import org.peelframework.core.beans.system.{Lifespan, System}
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.{Configurable, loadConfig}
import org.peelframework.core.graph.{Node, createGraph}
import org.peelframework.core.util.console._
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Execute all experiments in a suite. */
@Service("suite:run")
class Run extends Command {

  override val help = "execute all experiments in a suite"

  override def register(parser: Subparser) = {
    // options
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
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.suite.experiment.force", if (ns.getBoolean("app.suite.experiment.force")) "true" else "false")
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Running experiments in suite '${Sys.getProperty("app.suite.name")}'")

    val force = Sys.getProperty("app.suite.experiment.force", "false") == "true"
    val suite = context.getBean(Sys.getProperty("app.suite.name"), classOf[ExperimentSuite])
    val graph = createGraph(suite)

    //TODO check for cycles in the graph
    if (graph.isEmpty)
      throw new RuntimeException("Experiment suite is empty!")

    // resolve experiment configurations
    for (e <- suite.experiments)
      e.config = loadConfig(graph, e)

    // generate runs to be executed
    val runs = for {
      e <- suite.experiments
      i <- 1 to e.runs
      r = e.run(i, force)
      if force || !r.isSuccessful
    } yield r

    // filter experiments in the relevant runs
    val exps = runs.foldRight(List[Experiment[System]]())((r, es) =>
      if (es.isEmpty || es.head != r.exp) r.exp :: es
      else es)

    // SUITE lifespan
    try {
      logger.info("Executing experiments in suite")
      for (exp <- exps) {

        // traverse all systems relevant for this experiment
        def allSystems(reverse: Boolean) = for {
          System(s) <- if (reverse) graph.reverse.traverse() else graph.traverse()
          if graph.descendants(exp).contains(s)
        } yield s

        // traverse all systems relevant for this experiment
        // and required by at least one experiment input
        def inpSystems(reverse: Boolean) = {
          // compute the set of input descendants
          val inpDescendant = for {
            inp <- exp.inputs
            sys <- graph.descendants(inp)
          } yield sys
          allSystems(reverse = reverse).filter(inpDescendant)
        }

        // traverse all systems relevant for this experiment
        // and not required by any experiment input
        def expSystems(reverse: Boolean) = {
          // compute the set of experiment, non-input descendants
          val expDescendant = graph.descendants(exp, exp.inputs).toSet
          allSystems(reverse = reverse).filter(expDescendant)
        }

        // EXPERIMENT lifespan
        try {
          logger.info("#" * 60)
          logger.info("Current experiment is '%s'".format(exp.name))

          // update config
          for (Configurable(c) <- graph.descendants(exp))
            c.config = exp.config

          logger.info("Setting up / updating systems required for input data sets")
          for (s <- inpSystems(reverse = true))
            if (s.isUp) s.update()
            else s.setUp()

          logger.info("Materializing experiment input data sets")
          for {
            n <- exp.inputs
            p = n.resolve(n.path)
          } {
            if (!n.fs.exists(p)) {
              try {
                n.materialize()
              } catch {
                case e: Throwable =>
                  n.fs.rmr(p) // make sure the path is cleaned for the next try
                  throw e
              }
            } else {
              logger.info(s"Skipping already materialized path '$p'".yellow)
            }
          }

          logger.info("Tearing down redundant systems before conducting experiment runs")
          for {
            s <- inpSystems(reverse = false)
            if !(expSystems(reverse = false) contains s)
            if !(Seq(Lifespan.PROVIDED, Lifespan.SUITE) contains s.lifespan)
          } s.tearDown()

          logger.info("Setting up systems with SUITE lifespan")
          for {
            s <- allSystems(reverse = true)
            if (Seq(Lifespan.PROVIDED, Lifespan.SUITE) contains s.lifespan) && !s.isUp
          } s.setUp()

          logger.info("Updating systems with PROVIDED or SUITE lifespan")
          for {
            s <- allSystems(reverse = true)
            if Seq(Lifespan.PROVIDED, Lifespan.SUITE) contains s.lifespan
          } s.update()

          logger.info("Setting up systems with EXPERIMENT lifespan")
          for {
            System(s) <- expSystems(reverse = true)
            if Lifespan.EXPERIMENT == s.lifespan
          } s.setUp()

          for {
            r <- runs
            if r.exp == exp
          } {
            for (n <- exp.outputs)
              n.clean()

            logger.info("Setting up systems with RUN lifespan")
            for {
              s <- allSystems(reverse = false)
              if Lifespan.RUN == s.lifespan
            } s.setUp()

            try {
              // run experiment
              r.execute()
              // stop for a brief intermission between runs
              Thread.sleep(r.exp.config.getInt("experiment.run.intermission"))
            }
            finally {
              logger.info("Tearing down systems with RUN lifespan")
              for {
                s <- allSystems(reverse = false)
                if Lifespan.RUN == s.lifespan
              } s.tearDown()
            }

            for (n <- exp.outputs) n.clean()
          }

        } catch {
          case e: Throwable =>
            logger.error(s"Exception for experiment '${exp.name}' in suite '${suite.name}': ${e.getMessage}".red)
            throw e

        } finally {
          logger.info("Tearing down systems with EXPERIMENT lifespan")
          for {
            System(s) <- expSystems(reverse = false)
            if Lifespan.EXPERIMENT == s.lifespan
          } s.tearDown()
        }
      }

    }
    catch {
      case e: Throwable =>
        logger.error(s"Exception in suite '${suite.name}': ${e.getMessage}".red)
        throw e

    } finally {
      // Explicit shutdown only makes sense if at least one experiment is in the list,
      // otherwise the systems would not have been configured at all
      if (exps.nonEmpty) {
        logger.info("#" * 60)
        logger.info("Tearing down systems with SUITE lifespan")
        for {
          System(s) <- graph.traverse()
          if Lifespan.SUITE == s.lifespan
        } s.tearDown()
      }
    }
  }
}
