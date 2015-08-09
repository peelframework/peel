package eu.stratosphere.peel.core.cli.command.db

import java.lang.{System => Sys}
import java.nio.file.Paths

import eu.stratosphere.peel.core.beans.experiment.Experiment.RunName
import eu.stratosphere.peel.core.cli.command.Command
import eu.stratosphere.peel.core.config.loadConfig
import eu.stratosphere.peel.core.results.etl.{EventExtractorManager, ResultsETLSystem}
import EventExtractorManager.ProcessFile
import eu.stratosphere.peel.core.results.etl.traverser.{RunTraverser, SuiteTraverser}
import eu.stratosphere.peel.core.results.{model => db, _}
import eu.stratosphere.peel.core.util.console._
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Import suite results into an initialized database. */
@Service("db:import")
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

      // initialize ETL system
      val etlSystem = ResultsETLSystem(context, config)

      // extract, transform, and load events from files associated with successful experiments runs
      for ((suite, run) <- states zip runs if run.exit == 0; file <- RunTraverser(suitePath.resolve(suite.name))) {
        etlSystem ! ProcessFile(file, run)
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
