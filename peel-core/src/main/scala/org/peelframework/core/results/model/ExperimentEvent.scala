package org.peelframework.core.results.model

import java.time.Instant

/** Model class for experiment events extracted from the experiment run logs.
  *
  * @param experimentRunID The ID of the associated run.
  * @param name The name of the event.
  * @param task Optionally, the name of the associated task instance.
  * @param taskInstance Optionally, the number of the associated task instance.
  * @param vLong Optionally, an integer value for this event.
  * @param vDouble Optionally, a double value for this event.
  * @param vTimestamp Optionally, a timestamp for this event.
  * @param vString Optionally, a string value for this event.
  */
case class ExperimentEvent(
  experimentRunID: Int,
  name:            Symbol,
  task:            Option[String]  = Option.empty[String],
  taskInstance:    Option[Int]     = Option.empty[Int],
  vLong:           Option[Long]    = Option.empty[Long],
  vDouble:         Option[Double]  = Option.empty[Double],
  vTimestamp:      Option[Instant] = Option.empty[Instant],
  vString:         Option[String]  = Option.empty[String]
) {
  val id = this.##
}

/** [[ExperimentEvent]] companion and storage manager. */
object ExperimentEvent extends PersistedAPI[ExperimentEvent] {

  import java.sql.Connection

  import anorm.SqlParser._
  import anorm._

  override val tableName: String = "experiment_event"

  override val rowParser = {
    get[Int]             ("id")                 ~
    get[Int]             ("experiment_run_id")  ~
    get[String]          ("name")               ~
    get[Option[String]]  ("task")               ~
    get[Option[Int]]     ("task_instance")      ~
    get[Option[Long]]    ("v_long")             ~
    get[Option[Double]]  ("v_double")           ~
    get[Option[Instant]] ("v_timestamp")        ~
    get[Option[String]]  ("v_string")           map {
      case id ~ expRunID ~ name ~ task ~ inst ~ vLong ~ vDouble ~ vTimestamp ~ vString => ExperimentEvent(
        expRunID,
        Symbol(name),
        task,
        inst,
        vLong,
        vDouble,
        vTimestamp,
        vString)
    }
  }

  override def createTable()(implicit conn: Connection): Unit = if (!tableExists) {
    SQL( s"""
      CREATE TABLE experiment_event (
        id                 INTEGER        NOT NULL,
        experiment_run_id  INTEGER        NOT NULL,
        name               VARCHAR(63)    NOT NULL,
        task               VARCHAR(1024)          ,
        task_instance      INTEGER                ,
        v_long             BIGINT                 ,
        v_double           DOUBLE                 ,
        v_timestamp        TIMESTAMP              ,
        v_string           VARCHAR(1024)          ,
        PRIMARY KEY (id),
        FOREIGN KEY (experiment_run_id) REFERENCES experiment_run(id) ON DELETE CASCADE
      )""").execute()
  }

  override def insert(x: ExperimentEvent)(implicit conn: Connection): Unit = {
    SQL"""
    INSERT INTO experiment_event(id, experiment_run_id, name, task, task_instance, v_long, v_double, v_timestamp, v_string) VALUES(
      ${x.id},
      ${x.experimentRunID},
      ${x.name.name},
      ${x.task map (v => if (v.length < 1024) v else v.substring(0, 1024))},
      ${x.taskInstance},
      ${x.vLong},
      ${x.vDouble},
      ${x.vTimestamp},
      ${x.vString map (v => if (v.length < 1024) v else v.substring(0, 1024))}
    )
    """.executeInsert()
  }

  override def insert(xs: Seq[ExperimentEvent])(implicit conn: Connection): Unit = if (xs.nonEmpty) singleCommit {
    BatchSql(
      s"""
      INSERT INTO experiment_event(id, experiment_run_id, name, task, task_instance, v_long, v_double, v_timestamp, v_string) VALUES(
        {id},
        {experimentRunID},
        {name},
        {task},
        {taskInstance},
        {vLong},
        {vDouble},
        {vTimestamp},
        {vString}
      )
      """,
      namedParametersFor(xs.head),
      xs.tail.map(namedParametersFor): _*
    ).execute()
  }

  override def update(x: ExperimentEvent)(implicit conn: Connection): Unit = {
    throw new NotImplementedError("ExperimentEvent objects are immutable, update is not supported")
  }

  override def delete(x: ExperimentEvent)(implicit conn: Connection): Unit = {
    SQL"""
    DELETE FROM experiment_event WHERE id = ${x.id}
    """.execute()
  }

  def namedParametersFor(x: ExperimentEvent): Seq[NamedParameter] = Seq[NamedParameter](
    'id                -> x.id,
    'experimentRunID   -> x.experimentRunID,
    'name              -> x.name.name,
    'task              -> x.task,
    'taskInstance      -> x.taskInstance,
    'vLong             -> x.vLong,
    'vDouble           -> x.vDouble,
    'vTimestamp        -> x.vTimestamp,
    'vString           -> x.vString
  )
}