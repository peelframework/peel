package org.peelframework.core.results

import java.sql.{Connection, DriverManager}

import com.typesafe.config.Config
import org.peelframework.core.config.RichConfig
import org.peelframework.core.results.model._
import org.slf4j.{Logger, LoggerFactory}

/** DB object.
  */
object DB {

  val logger: Logger = LoggerFactory.getLogger("org.peelframework.core.db")

  // try to load the MonetDB JDBC drivers, which currently lacks an autoload descriptor
  // see https://www.monetdb.org/bugzilla/show_bug.cgi?id=3748 for a proposed fix
  loadDriver("nl.cwi.monetdb.jdbc.MonetDriver")

  /** Silently tries to load a JDBC driver with the given `className`.
    *
    * @param className The FQName of the driver to be loaded.
    */
  private def loadDriver(className: String): Unit = try {
    Class.forName(className)
  } catch {
    case _: Throwable => // silently ignore exception
  }

  /** Creates a database connection using the 'app.db.$connName.conf' connection data.
    *
    * @param name The name of the connection configuration
    * @param config The config object holding the config data
    * @return
    */
  def getConnection(name: String)(implicit config: Config): Connection = {
    val url = config.getString(s"app.db.$name.url")
    val user = config.getOptionalString(s"app.db.$name.user")
    val pass = config.getOptionalString(s"app.db.$name.password")
    DriverManager.getConnection(url, user.getOrElse(null.asInstanceOf[String]), pass.getOrElse(null.asInstanceOf[String]))
  }

  /** Initialize the database schema.
    *
    * @param conn The DB connection.
    */
  def dropSchema(silent: Boolean = false)(implicit conn: Connection): Unit = {
    if (!silent) logger.info(s"Dropping table ${ExperimentEvent.tableName}")
    ExperimentEvent.dropTable()
    if (!silent) logger.info(s"Dropping table ${ExperimentRun.tableName}")
    ExperimentRun.dropTable()
    if (!silent) logger.info(s"Dropping table ${Experiment.tableName}")
    Experiment.dropTable()
    if (!silent) logger.info(s"Dropping table ${System.tableName}")
    System.dropTable()
  }

  /** Initialize the database schema.
    *
    * @param conn The DB connection.
    */
  def createSchema(silent: Boolean = false)(implicit conn: Connection): Unit = {
    if (!silent) logger.info(s"Creating table ${System.tableName}")
    System.createTable()
    if (!silent) logger.info(s"Creating table ${Experiment.tableName}")
    Experiment.createTable()
    if (!silent) logger.info(s"Creating table ${ExperimentRun.tableName}")
    ExperimentRun.createTable()
    if (!silent) logger.info(s"Creating table ${ExperimentEvent.tableName}")
    ExperimentEvent.createTable()
  }
}
