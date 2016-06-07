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
package org.peelframework.core.results.model

import java.nio.file.Path
import java.time.Instant

/** Model class for experiment events extracted from the experiment run logs.
  *
  * @param id The unique ID for this event instance.
  * @param experimentRunID The ID of the associated run.
  * @param name The name of the event.
  * @param host Optionally, the host where the event occurs.
  * @param task Optionally, the name of the associated task instance.
  * @param taskInstance Optionally, the number of the associated task instance.
  * @param vLong Optionally, an integer value for this event.
  * @param vDouble Optionally, a double value for this event.
  * @param vTimestamp Optionally, a timestamp for this event.
  * @param vString Optionally, a string value for this event.
  */
case class ExperimentEvent(
  id:              Long,
  experimentRunID: Int,
  name:            Symbol,
  host:            Option[String]  = Option.empty[String],
  task:            Option[String]  = Option.empty[String],
  taskInstance:    Option[Int]     = Option.empty[Int],
  vLong:           Option[Long]    = Option.empty[Long],
  vDouble:         Option[Double]  = Option.empty[Double],
  vTimestamp:      Option[Instant] = Option.empty[Instant],
  vString:         Option[String]  = Option.empty[String]
)

/** [[ExperimentEvent]] companion and storage manager. */
object ExperimentEvent extends PersistedAPI[ExperimentEvent] {

  import java.sql.Connection

  import anorm.SqlParser._
  import anorm._

  override val tableName: String = "experiment_event"

  override val rowParser = {
    get[Long]            ("id")                 ~
    get[Int]             ("experiment_run_id")  ~
    get[String]          ("name")               ~
    get[Option[String]]  ("host")               ~
    get[Option[String]]  ("task")               ~
    get[Option[Int]]     ("task_instance")      ~
    get[Option[Long]]    ("v_long")             ~
    get[Option[Double]]  ("v_double")           ~
    get[Option[Instant]] ("v_timestamp")        ~
    get[Option[String]]  ("v_string")           map {
      case id ~ expRunID ~ name ~ host ~ task ~ inst ~ vLong ~ vDouble ~ vTimestamp ~ vString => ExperimentEvent(
        id,
        expRunID,
        Symbol(name),
        host,
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
        id                 BIGINT         NOT NULL,
        experiment_run_id  INTEGER        NOT NULL,
        name               VARCHAR(63)    NOT NULL,
        host               VARCHAR(127)           ,
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
    INSERT INTO experiment_event(id, experiment_run_id, name, host, task, task_instance, v_long, v_double, v_timestamp, v_string) VALUES(
      ${x.id},
      ${x.experimentRunID},
      ${x.name.name},
      ${x.host map (v => if (v.length < 127) v else v.substring(0, 127))},
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
      INSERT INTO experiment_event(id, experiment_run_id, name, host, task, task_instance, v_long, v_double, v_timestamp, v_string) VALUES(
        {id},
        {experimentRunID},
        {name},
        {host},
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

  def importCSV(path: Path, fsep: Char, lsep: Char, quote: Char, nil: String)(implicit conn: Connection): Unit = {
    val stmts = conn.getClass.getCanonicalName match {
      case "nl.cwi.monetdb.jdbc.MonetConnection" => Seq(
        s"""
           |COPY INTO experiment_event FROM '$path'
           |USING DELIMITERS '$fsep', '$lsep', '$quote' NULL AS '$nil';
         """
      )

      case "org.h2.jdbc.JdbcConnection" => Seq(
        s"""
           |SET LOG 0
         """,
        s"""
           |SET LOCK_MODE 0
         """,
        s"""
           |SET UNDO_LOG 0
         """,
        s"""
           |INSERT INTO experiment_event
           |DIRECT SELECT * FROM CSVREAD('$path', '${cols(fsep)}', 'charset=UTF-8 fieldSeparator=$fsep')
         """,
        s"""
           |SET UNDO_LOG 1
         """,
        s"""
           |SET LOCK_MODE 1
         """,
        s"""
           |SET LOG 1
         """
      )

      case fqName =>
        throw new IllegalStateException(s"Cannot import CSV data into unsupported backend '$fqName'")
    }

    for (stmt <- stmts)
      SQL(stmt.stripMargin.trim).execute()
  }

  def namedParametersFor(x: ExperimentEvent): Seq[NamedParameter] = Seq[NamedParameter](
    'id                -> x.id,
    'experimentRunID   -> x.experimentRunID,
    'name              -> x.name.name,
    'host              -> x.host,
    'task              -> x.task,
    'taskInstance      -> x.taskInstance,
    'vLong             -> x.vLong,
    'vDouble           -> x.vDouble,
    'vTimestamp        -> x.vTimestamp,
    'vString           -> x.vString
  )

  private def cols(fsep: Char) = Seq(
    "id",
    "experiment_run_id",
    "name",
    "host",
    "task",
    "task_instance",
    "v_long",
    "v_double",
    "v_timestamp",
    "v_string").mkString(fsep.toString)
}