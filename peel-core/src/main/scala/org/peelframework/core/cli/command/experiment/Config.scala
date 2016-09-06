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

import com.typesafe.config.ConfigRenderOptions
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.loadConfig
import org.peelframework.core.graph.createGraph
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Execute a specific experiment. */
@Service("exp:config")
class Config extends Command {

  override val help = "list the configuration of a specific experiment"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--json")
      .`type`(classOf[Boolean])
      .dest("app.render.config.json")
      .action(Arguments.storeTrue())
      .help("print with JSON syntax (otherwise HOCON)")
    parser.addArgument("--comments")
      .`type`(classOf[Boolean])
      .dest("app.render.config.comments")
      .action(Arguments.storeTrue)
      .help("print comments")
    parser.addArgument("--origin")
      .`type`(classOf[Boolean])
      .dest("app.render.config.origin")
      .action(Arguments.storeTrue)
      .help("print origin")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("suite containing the experiment")
    parser.addArgument("experiment")
      .`type`(classOf[String])
      .dest("app.suite.exp.name")
      .metavar("EXPERIMENT")
      .help("experiment to run")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.render.config.json", if (ns.getBoolean("app.render.config.json")) "true" else "false")
    Sys.setProperty("app.render.config.comments", if (ns.getBoolean("app.render.config.comments")) "true" else "false")
    Sys.setProperty("app.render.config.origin", if (ns.getBoolean("app.render.config.origin")) "true" else "false")
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
    Sys.setProperty("app.suite.exp.name", ns.getString("app.suite.exp.name"))
  }

  override def run(context: ApplicationContext) = {
    val suiteName = Sys.getProperty("app.suite.name")
    val expName = Sys.getProperty("app.suite.exp.name")
    val json = Sys.getProperty("app.render.config.json") == "true"
    val comments = Sys.getProperty("app.render.config.comments") == "true"
    val origin = Sys.getProperty("app.render.config.origin") == "true"

    logger.info(s"Listing configuration for experiment '${Sys.getProperty("app.suite.exp.name")}' from suite '${Sys.getProperty("app.suite.name")}'")

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

    val renderOptions = ConfigRenderOptions.defaults()
      .setFormatted(true)
      .setOriginComments(origin)
      .setComments(comments)
      .setJson(json)

    for (exp <- exps) {
      println("-" * 80)
      println(exp.config.root.render(renderOptions))
      println("-" * 80)
    }

  }
}
