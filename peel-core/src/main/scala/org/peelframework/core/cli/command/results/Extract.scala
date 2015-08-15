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
package org.peelframework.core.cli.command.results

import java.io.File
import java.lang.{System => Sys}

import org.peelframework.core.cli.command.Command
import org.peelframework.core.config._
import org.peelframework.core.util.console.ConsoleColorise
import org.peelframework.core.util.shell
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Extract suite results from a tar.gz. */
@Service("res:extract")
class Extract extends Command {

  override val name = "res:extract"

  override val help = "extract suite results from a tar.gz"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--force", "-f")
      .`type`(classOf[Boolean])
      .dest("app.suite.extract.results.force")
      .action(Arguments.storeTrue)
      .help("force archive update")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite to archive")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.suite.extract.results.force", if (ns.getBoolean("app.suite.extract.results.force")) "true" else "false")
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    implicit val config = loadConfig() // load application configuration

    val force = config.getString("app.suite.extract.results.force") == "true" // get force flag from command parameter
    val suite = config.getString("app.suite.name") // get suite from command parameter

    logger.info(s"Archiving results for suite '${Sys.getProperty("app.suite.name")}'")

    val resFile = new File(config.getString("app.path.results"), suite)
    val zipFile = new File(config.getString("app.path.results"), s"$suite.tar.gz")

    if (zipFile.exists()) /* archive exists */ {
      assert(zipFile.isFile, s"Results archive '$zipFile' is not a normal file")

      if (force || !resFile.exists()) {
        logger.info(s"Extracting results from '$zipFile' to '${resFile.getParent}'")
        shell.extract(zipFile.toString, resFile.getParent)
        logger.info(s"Removing extracted archive '$zipFile'")
        shell.rm(zipFile.toString)
      } else {
        logger.info(s"Skipping extraction for already existing folder '$resFile' (use '-f' to force)".yellow)
      }

    } else /* archive is missing */ {
      logger.info(s"Skipping extraction for non-existing archive '$zipFile'".yellow)
    }
  }
}
