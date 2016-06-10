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
package org.peelframework.core.cli.command.db

import java.lang.{System => Sys}

import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.loadConfig
import org.peelframework.core.results.DB
import org.peelframework.core.util.console._
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Initialize results database. */
@Service("db:initialize")
class Initialize extends Command {

  override val help = "initialize results database"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--connection")
      .`type`(classOf[String])
      .dest("app.db.connection")
      .metavar("ID")
      .help("database config name (default: `default`)")
    parser.addArgument("--force", "-f")
      .`type`(classOf[Boolean])
      .dest("app.db.force")
      .action(Arguments.storeTrue)
      .help("force creation (drops existing tables)")

    // option defaults
    parser.setDefault("app.db.connection", "default")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.db.connection", ns.getString("app.db.connection"))
    Sys.setProperty("app.db.force", if (ns.getBoolean("app.db.force")) "true" else "false")
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Initialising results database schema in database '${Sys.getProperty("app.db.connection")}'")

    // get force flag
    val force = Sys.getProperty("app.db.force") == "true"

    // load application configuration
    implicit val config = loadConfig()

    // create database connection
    val connName = Sys.getProperty("app.db.connection")
    implicit val conn = DB.getConnection(connName)

    try {
      if (force) {
        DB.dropSchema()
      }

      DB.createSchema()
    }
    catch {
      case e: Throwable =>
        logger.error(s"Error while initializing database '$connName': ${e.getMessage}".red)
        throw e
    } finally {
      logger.info(s"Closing connection to database '$connName'")
      conn.close()
      logger.info("#" * 60)
    }
  }
}
