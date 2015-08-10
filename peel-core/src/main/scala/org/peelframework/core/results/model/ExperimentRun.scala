package org.peelframework.core.results.model

/** Model class for experiment runs.
  *
  * Captures information for a single [[org.peelframework.core.beans.experiment.Experiment.Run Experiment.Run]].
  *
  * @param experimentID The ID of the parent [[org.peelframework.core.results.model.Experiment Experiment]].
  * @param run The run number.
  * @param time The wall clock time for this run (in milliseconds).
  */
case class ExperimentRun(
  experimentID: Int,
  run: Int,
  exit: Int,
  time: Long
) {
  val id = experimentID.## * 31 + run
}

/** [[ExperimentRun]] companion and storage manager. */
object ExperimentRun extends PersistedAPI[ExperimentRun] {

  import java.sql.Connection

  import anorm.SqlParser._
  import anorm._

  override val tableName = "experiment_run"

  override val rowParser = {
    get[Int]  ("id")            ~
    get[Int]  ("experiment_id") ~
    get[Int]  ("run")           ~
    get[Int]  ("exit")          ~
    get[Long] ("time")          map {
      case id ~ experiment_id ~ run ~ exit ~ time => ExperimentRun(experiment_id, run, exit, time)
    }
  }

  override def createTable()(implicit conn: Connection): Unit = if (!tableExists) {
    SQL( s"""
      CREATE TABLE experiment_run (
        id             INTEGER  NOT NULL,
        experiment_id  INTEGER  NOT NULL,
        run            INTEGER  NOT NULL,
        exit           INTEGER  NOT NULL,
        time           BIGINT   NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (experiment_id) REFERENCES experiment(id) ON DELETE CASCADE
      )""").execute()
  }

  override def insert(x: ExperimentRun)(implicit conn: Connection): Unit = {
    SQL"""
    INSERT INTO experiment_run(id, experiment_id, run, exit, time) VALUES(
      ${x.id},
      ${x.experimentID},
      ${x.run},
      ${x.exit},
      ${x.time}
    )
    """.executeInsert()
  }

  override def update(x: ExperimentRun)(implicit conn: Connection): Unit = {
    SQL"""
    UPDATE experiment_run SET
      experiment_id = ${x.experimentID},
      run           = ${x.run},
      exit          = ${x.exit},
      time          = ${x.time}
    WHERE
      id            = ${x.id}
    """.executeUpdate()
  }

  override def delete(x: ExperimentRun)(implicit conn: Connection): Unit = {
    SQL"""
    DELETE FROM experiment_run WHERE id = ${x.id}
    """.execute()
  }
}