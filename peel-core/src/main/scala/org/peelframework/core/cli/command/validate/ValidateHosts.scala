/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
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
package org.peelframework.core.cli.command.validate

import java.lang.{System => Sys}

import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.core.beans.system.System
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config._
import org.peelframework.core.graph._
import org.peelframework.core.util.console._
import org.peelframework.core.util.shell
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

import scala.collection.JavaConversions._

/** Validates correct hosts setup. */
@Service("val:hosts")
class ValidateHosts extends Command {

  override val name = "val:hosts"

  override val help = "validates correct hosts setup"

  override def register(parser: Subparser) = {
    // arguments
    parser.addArgument("--suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("suite containing the experiment")
    parser.addArgument("--experiment")
      .`type`(classOf[String])
      .dest("app.suite.experiment.name")
      .metavar("EXPERIMENT")
      .help("experiment to run")

    // option defaults
    parser.setDefault("app.suite.name", "")
    parser.setDefault("app.suite.experiment.name", "")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.suite.experiment.name", ns.getString("app.suite.experiment.name"))
  }

  override def run(context: ApplicationContext): Unit = {

    val suiteName = Sys.getProperty("app.suite.name")
    val expName = Sys.getProperty("app.suite.experiment.name")

    val suites: Map[String, ExperimentSuite] =
      if (suiteName == "" && expName == "") {
        context.getBeansOfType(classOf[ExperimentSuite]).toMap
      } else {
        Map(suiteName -> context.getBean(suiteName, classOf[ExperimentSuite]))
      }

    for (suite <- suites.values) {
      val graph = createGraph(suite)

      if (graph.isEmpty) throw new RuntimeException("Experiment suite is empty!")

      // find experiment or take complete suite
      val exps =
        if (expName == "")
          suite.experiments
        else
          suite.experiments.filter(_.name == expName)

      // load config(s)
      for (e <- exps) e.config = loadConfig(graph, e)

      // check if experiment exists (the list should contain exactly one element)
      if (exps.size < 1) throw new RuntimeException(s"Experiment '$expName' either not found or ambiguous in suite '$suiteName'")

      for (exp <- exps) {
        logger.info(s"Validating configuration for experiment '${exp.name}'")

        val config = exp.config
        val systems = for (
          n <- graph.reverse.traverse()
          if graph.descendants(exp).contains(n) && n.isInstanceOf[System]
        ) yield n.asInstanceOf[System]

        // gather masters, avoid duplicates
        val masters = {
          val builder = Set.newBuilder[String]
          for {
            sys <- systems
            path = s"system.${sys.configKey}.config.masters"
            if config.hasPath(path)
          } builder ++= config.getStringList(path).toList
          builder.result()
        }
        // gather slaves, avoid duplicates
        val slaves = {
          val builder = Set.newBuilder[String]
          for {
            sys <- systems
            path = s"system.${sys.configKey}.config.slaves"
            if config.hasPath(path)
          } builder ++= config.getStringList(path).toList
          builder.result()
        }

        for (slave <- (masters union slaves).toSeq.sorted) {
          logger.info(s"Validating host '$slave'")
          val known = shell ! s"ssh-keygen -H -F $slave" == 0
          if (!known) {
            logger.error(s"Host '$slave' is not in the list of known hosts".red)
          } else {
            // check authorized keys
            val authorized = shell ! s"ssh -o BatchMode=yes $slave exit" == 0
            if (!authorized) logger.error(s"Host '$slave' is not in the list of authorized keys".red)
          }
        }
      }
    }
  }
}
