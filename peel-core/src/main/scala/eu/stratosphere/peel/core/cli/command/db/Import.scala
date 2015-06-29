package eu.stratosphere.peel.core.cli.command.db

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.experiment.Experiment.RunName
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.loadConfig
import eu.stratosphere.peel.core.results.etl.SuiteTraverser
import eu.stratosphere.peel.core.results.{model => db, _}
import eu.stratosphere.peel.core.util.console._
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext

class Import extends Command {

  override val name = "db:import"

  override val help = "import suite results into an initialized database"

  override def register(parser: Subparser) = {
    // options
    parser.addArgument("--connection")
      .`type`(classOf[String])
      .dest("app.db.connection")
      .metavar("ID")
      .help("database config name (default: h2)")
    parser.addArgument("--force", "-f")
      .`type`(classOf[Boolean])
      .dest("app.db.force")
      .action(Arguments.storeTrue)
      .help("force deletion of old experiments in the DB")
    parser.addArgument("--experiments")
      .`type`(classOf[String])
      .dest("app.path.experiments")
      .metavar("EXPFILE")
      .help("experiments file (default: config/experiments.xml)")
    // arguments
    parser.addArgument("suite")
      .`type`(classOf[String])
      .dest("app.suite.name")
      .metavar("SUITE")
      .help("experiments suite to run")

    // option defaults
    parser.setDefault("app.db.connection", "h2")
    parser.setDefault("app.path.experiments", "config/experiments.xml")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.db.connection", ns.getString("app.db.connection"))
    Sys.setProperty("app.path.experiments", Paths.get(ns.getString("app.path.experiments")).normalize.toAbsolutePath.toString)
    Sys.setProperty("app.suite.name", ns.getString("app.suite.name"))
  }

  override def run(context: ApplicationContext) = {
    logger.info(s"Importing results from suite '${Sys.getProperty("app.suite.name")}' into '${Sys.getProperty("app.db.connection")}'")

    // get force flag
    val force = Sys.getProperty("app.db.force") == "true"

    // load application configuration
    implicit val config = loadConfig()

    // create database connection
    val connName = Sys.getProperty("app.db.connection")
    implicit val conn = DB.getConnection(connName)

    val suite = Symbol(config.getString("app.suite.name"))

    logger.info(s"Traversing experiment runs in suite ${suite.name}")
    try {
      // collect run state data
      val states = SuiteTraverser(suite).toSeq.sortBy(_.name)

      val systems = {
        // extract objects
        val xs = for {
          s <- states
        } yield db.System(Symbol(s.runnerID), Symbol(s.runnerName), Symbol(s.runnerVersion))
        // remove duplicates and sort
        xs.toSet[db.System].toSeq.sortBy(_.id.name)
      }
      val experiments = {
        // extract objects
        val xs = for {
          s <- states; (expName, runNo) <- RunName.unapply(s.name)
        } yield db.Experiment(Symbol(expName), suite, Symbol(s.runnerID))
        xs.toSet[db.Experiment].toSeq.sortBy(_.name.name)
      }
      val runs = {
        // construct auxiliary "name -> experiment" map
        val em = Map((for (e <- experiments) yield e.name -> e): _*)
        // extract objects
        val xs = for {
          s <- states; (expName, runNo) <- RunName.unapply(s.name); exp <- em.get(Symbol(expName))
        } yield db.ExperimentRun(exp.id, runNo, s.runExitCode.getOrElse(-1), s.runTime)
        xs.toSeq
      }

      // delete old experiments and associated data
      db.Experiment.delete(experiments)
      // insert new systems, experiments, and runs
      db.System.insertMissing(systems)
      db.Experiment.insert(experiments)
      db.ExperimentRun.insert(runs)

      // load available event extractor factories
//      val eventExtractors = ServiceLoader.load[EventExtractorFactory](classOf[EventExtractorFactory]).iterator().toSeq

//      val xs = for {
//        state     <- states
//        file      <- RunTraverser(state)
//        extractor <- eventExtractors
//        format    <- extractor.format(state, file).toSeq
//        handler   <- extractor.handlers(state, file)
//      } yield (format, handler)
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
