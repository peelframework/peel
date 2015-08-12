/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
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

/** Model class for systems.
  *
  * Captures information for a [[org.peelframework.core.beans.system.System System]] involved in an
  * [[org.peelframework.core.beans.experiment.Experiment Experiment]] execution.
  *
  * @param id The `id` of the bean.
  * @param name The value of the bean `name` property.
  * @param version The value of the bean `version` property.
  */
case class System(
  id: Symbol,
  name: Symbol,
  version: Symbol
)

/** [[System]] companion and storage manager. */
object System extends PersistedAPI[System] {

  import java.sql.Connection

  import anorm.SqlParser._
  import anorm._

  override val tableName = "system"

  override val rowParser = {
    get[String] ("id")      ~
    get[String] ("name")    ~
    get[String] ("version") map {
      case id ~ name ~ version => System(Symbol(id), Symbol(name), Symbol(version))
    }
  }

  override def createTable()(implicit conn: Connection): Unit = if (!tableExists) {
    SQL( s"""
    CREATE TABLE $tableName (
      id      VARCHAR(63) NOT NULL,
      name    VARCHAR(63) NOT NULL,
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
      name    = ${x.name.name},
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
