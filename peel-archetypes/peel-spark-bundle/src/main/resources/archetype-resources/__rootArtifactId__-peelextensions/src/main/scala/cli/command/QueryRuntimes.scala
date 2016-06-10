#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.cli.command

import java.lang.{System => Sys}

import anorm.SqlParser._
import anorm._
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.loadConfig
import org.peelframework.core.results.DB
import org.peelframework.core.util.console._
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Query the database for the runtimes of a particular experiment. */
@Service("query:runtimes")
class QueryRuntimes extends Command {

  import scala.language.postfixOps

  override val name = "query:runtimes"

  override val help = "query the database for the runtimes of a particular experiment."

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
    logger.info(s"Querying runtime results for suite '${symbol_dollar}{Sys.getProperty("app.suite.name")}' from '${symbol_dollar}{Sys.getProperty("app.db.connection")}'")

    // load application configuration
    implicit val config = loadConfig()

    // create database connection
    val connName = Sys.getProperty("app.db.connection")
    implicit val conn = DB.getConnection(connName)

    val suite = config.getString("app.suite.name")

    try {
      // Create an SQL query
      val runtimes = SQL(
          """
          |SELECT   e.suite                                 as suite       ,
          |         e.name                                  as name        ,
          |         MIN(r.time)                             as min_time    ,
          |         MAX(r.time)                             as max_time    ,
          |         SUM(r.time) - MIN(r.time) - MAX(r.time) as median_time
          |FROM     experiment                              as e           ,
          |         experiment_run                          as r
          |WHERE    e.id    = r.experiment_id
          |AND      e.suite = {suite}
          |GROUP BY e.suite, e.name
          |ORDER BY e.suite, e.name
          """.stripMargin.trim
        )
        .on("suite" -> suite)
        .as({
          get[String] ("suite")       ~
          get[String] ("name")        ~
          get[Int]    ("min_time")    ~
          get[Int]    ("max_time")    ~
          get[Int]    ("median_time") map {
            case _ ~ name ~ min ~ max ~ median => (suite, name, min, max, median)
          }
        } * )

      logger.info(s"------------------------------------------------------------------------------------------------")
      logger.info(s"| RUNTIME RESULTS FOR '${symbol_dollar}suite' ${symbol_dollar}{" " * (69 - suite.length)} |")
      logger.info(s"------------------------------------------------------------------------------------------------")
      logger.info(s"| name                      | name                      |        min |        max |     median |")
      logger.info(s"------------------------------------------------------------------------------------------------")
      for ((suite, name, min, max, median) <- runtimes) {
        logger.info(f"| ${symbol_dollar}suite%-25s | ${symbol_dollar}name%-25s | ${symbol_dollar}min%10d | ${symbol_dollar}max%10d | ${symbol_dollar}median%10d | ")
      }
      logger.info(s"------------------------------------------------------------------------------------------------")
    }
    catch {
      case e: Throwable =>
        logger.error(s"Error while querying runtime results for suite '${symbol_dollar}suite'".red)
        throw e
    } finally {
      logger.info(s"Closing connection to database '${symbol_dollar}connName'")
      conn.close()
      logger.info("${symbol_pound}" * 60)
    }
  }
}
