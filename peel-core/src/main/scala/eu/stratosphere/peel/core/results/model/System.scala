package eu.stratosphere.peel.core.results.model

/** Model class for systems.
  *
  * Captures information for a [[eu.stratosphere.peel.core.beans.system.System System]] involved in an
  * [[eu.stratosphere.peel.core.beans.experiment.Experiment Experiment]] execution.
  *
  * @param id The `id` of the bean.
  * @param name The value of the bean `name` property.
  * @param version The value of the bean `version` property.
  */
case class System(
  id: Symbol,
  name: Symbol,
  version: Symbol
) {}

object System extends PersistedAPI[System] {

  import java.sql.Connection

  import anorm.SqlParser._
  import anorm._

  override val tableName = "system"

  override val rowParser = {
    get[String]("id") ~ get[String]("name") ~ get[String]("version") map {
      case id ~ name ~ version => System(Symbol(id), Symbol(name), Symbol(version))
    }
  }

  override def createTable()(implicit conn: Connection): Unit = if (!tableExists) {
    SQL( s"""
    CREATE TABLE $tableName (
      id VARCHAR(63) NOT NULL,
      name VARCHAR(63) NOT NULL,
      version VARCHAR(63) NOT NULL,
      PRIMARY KEY (id)
    )""").execute()
  }

  override def insert(x: System)(implicit conn: Connection): Unit = {
    SQL"""
    INSERT INTO system(id, name, version) VALUES(
      ${x.id.name},
      ${x.name.name},
      ${x.version.name}
    )
    """.executeInsert()
  }

  override def update(x: System)(implicit conn: Connection): Unit = {
    SQL"""
    UPDATE system SET
      name = ${x.name.name},
      version = ${x.version.name}
    WHERE
      id = ${x.id.name}
    """.executeUpdate()
  }

  override def delete(x: System)(implicit conn: Connection): Unit = {
    SQL"""
    DELETE FROM system WHERE id = ${x.id.name}
    """.execute()
  }
}
