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
import java.nio.file.Paths

import org.peelframework.core.beans.experiment.Experiment.RunName
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.loadConfig
import org.peelframework.core.results.etl.{EventExtractorManager, ResultsETLSystem}
import EventExtractorManager.ProcessFile
import org.peelframework.core.results.etl.traverser.{RunTraverser, SuiteTraverser}
import org.peelframework.core.results.{model => db, _}
import org.peelframework.core.util.console._
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Import suite results into an initialized database. */
@Service("db:import")
class Import extends Command {

  override val help = "import suite results into an initialized database"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--connection")
      .`type`(classOf[String])
      .dest("app.db.connection")
      .metavar("ID")
      .help("database config name (default: `default`)")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite to run")

    // option defaults
    parser.setDefault("app.db.connection", "default")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.db.connection", ns.getString("app.db.connection"))
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Importing results from suite '${Sys.getProperty("app.suite.name")}' into '${Sys.getProperty("app.db.connection")}'")

    // load application configuration
    implicit val config = loadConfig()

    // create database connection
    val connName = Sys.getProperty("app.db.connection")
    implicit val conn = DB.getConnection(connName)

    val suite = Symbol(config.getString("app.suite.name"))

    // resolve paths
    val resultsPath = Paths.get(config.getString("app.path.results")).normalize.toAbsolutePath
    val suitePath = Paths.get(resultsPath.toString, suite.name)

    try {
      if (!suitePath.toFile.isDirectory) {
        throw new IllegalArgumentException(s"Suite folder '${suite.name}' does not exist in '$resultsPath'")
      }

      logger.info(s"Traversing experiment runs in suite ${suite.name}")

      // collect run state data
      val states = SuiteTraverser(suitePath).toSeq sortBy (_.name)

      // collect systems
      val systems = {
        // extract objects
        val xs = for {
          s <- states
        } yield db.System(Symbol(s.runnerID), Symbol(s.runnerName), Symbol(s.runnerVersion))
        // remove duplicates and sort
        xs.toSet[db.System].toSeq.sortBy(_.id.name)
      }

      // collect experiments
      val experiments = {
        // extract objects
        val xs = for {
          s <- states; (expName, runNo) <- RunName.unapply(s.name)
        } yield db.Experiment(Symbol(expName), suite, Symbol(s.runnerID))
        xs.toSet[db.Experiment].toSeq.sortBy(_.name.name)
      }

      // collect runs
      val runs = {
        // construct auxiliary "name -> experiment" map
        val em = Map((for (e <- experiments) yield e.name -> e): _*)
        // extract objects
        for {
          s <- states; (expName, runNo) <- RunName.unapply(s.name); exp <- em.get(Symbol(expName))
        } yield db.ExperimentRun(exp.id, runNo, s.runExitCode.getOrElse(-1), s.runTime)
      }

      // delete old experiments and associated data
      db.Experiment.delete(experiments)

      // insert new systems, experiments, and runs
      db.System.insertMissing(systems)
      db.Experiment.insert(experiments)
      db.ExperimentRun.insert(runs)

      // initialize ETL system
      val etlSystem = ResultsETLSystem(context, config)

      // extract, transform, and load events from files associated with successful experiments runs
      for {
        (state, run) <- states zip runs if run.exit == 0
        basePath     =  suitePath.resolve(state.name)
        file         <- RunTraverser(suitePath.resolve(state.name))
      } {
        etlSystem ! ProcessFile(basePath, file, run)
      }

      // shutdown the system
      etlSystem.shutdown()
    }
    catch {
      case e: Throwable =>
        logger.error(s"Error while loading suite '${suite.name}' in database '$connName': ${e.getMessage}".red)
        throw e
    } finally {
      logger.info(s"Closing connection to database '$connName'")
      conn.close()
      logger.info("#" * 60)
    }
  }
}
