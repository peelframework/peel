package eu.stratosphere.peel.core.results.model

/** Model class for experiments.
  *
  * Captures information for an [[eu.stratosphere.peel.core.beans.experiment.Experiment Experiment]] bean execution.
  *
  * @param name The value of the bean `name` property.
  * @param suite The `id` value of the enclosing [[eu.stratosphere.peel.core.beans.experiment.ExperimentSuite ExperimentSuite]] bean.
  * @param system The `id` value of the associated runner [[eu.stratosphere.peel.core.beans.experiment.Experiment Experiment]] bean.
  */
case class Experiment(
  name: Symbol,
  suite: Symbol,
  system: Symbol
) {
  val id = name.## * 31 + suite.##
}

/** [[Experiment]] companion and storage manager. */
object Experiment extends PersistedAPI[Experiment] {

  import java.sql.Connection

  import anorm.SqlParser._
  import anorm._

  override val tableName = "experiment"

  override val rowParser = {
    get[Int]    ("id")     ~
    get[String] ("name")   ~
    get[String] ("suite")  ~
    get[String] ("system") map {
      case id ~ name ~ suite ~ system => Experiment(Symbol(name), Symbol(suite), Symbol(system))
    }
  }

  override def createTable()(implicit conn: Connection): Unit = if (!tableExists) {
    SQL( s"""
      CREATE TABLE experiment (
        id     INTEGER     NOT NULL,
        name   VARCHAR(63) NOT NULL,
        suite  VARCHAR(63) NOT NULL,
        system VARCHAR(63) NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (system) REFERENCES system(id) ON DELETE RESTRICT
      )""").execute()
  }

  override def insert(x: Experiment)(implicit conn: Connection): Unit = {
    SQL"""
    INSERT INTO experiment(id, name, suite, system) VALUES(
      ${x.id},
      ${x.name.name},
      ${x.suite.name},
      ${x.system.name}
    )
    """.executeInsert()
  }

  override def insert(xs: Seq[Experiment])(implicit conn: Connection): Unit = if (xs.nonEmpty) {
    BatchSql(
      s"""
      INSERT INTO experiment(id, name, suite, system) VALUES(
        {id},
        {name},
        {suite},
        {system}
      )
      """,
      namedParametersFor(xs.head),
      xs.tail.map(namedParametersFor): _*
    ).execute()
  }

  override def insertMissing(xs: Seq[Experiment])(implicit conn: Connection) = {
    val current = selectAll().toSet // find current experiments
    insert(xs.filterNot(current.contains)) // insert the ones which are not in the current list
  }

  override def update(x: Experiment)(implicit conn: Connection): Unit = {
    SQL"""
    UPDATE experiment SET
      name    = ${x.name.name},
      suite   = ${x.suite.name},
      system  = ${x.system.name}
    WHERE
      id      = ${x.id}
    """.executeUpdate()
  }

  override def delete(x: Experiment)(implicit conn: Connection): Unit = {
    SQL"""
    DELETE FROM experiment WHERE id = ${x.id}
    """.execute()
  }

  def namedParametersFor(x: Experiment): Seq[NamedParameter] = Seq[NamedParameter](
    'id     -> x.id,
    'name   -> x.name.name,
    'suite  -> x.suite.name,
    'system -> x.system.name
  )
}