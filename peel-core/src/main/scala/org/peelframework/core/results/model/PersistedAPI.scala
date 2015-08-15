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

/** A common API for persisted objects.
  *
  * @tparam A The type of persisted objects
  */
trait PersistedAPI[A] {

  import java.sql.Connection

  import anorm._

  import language.postfixOps

  val tableName: String

  val rowParser: RowParser[A]

  def tableExists(implicit conn: Connection) = {
    conn.getMetaData.getTables(conn.getCatalog, null, tableName.toUpperCase, null).next()
  }

  def dropTable()(implicit conn: Connection): Unit = if (tableExists) {
    SQL(s"DROP TABLE $tableName").execute()
  }

  def createTable()(implicit conn: Connection): Unit

  def selectAll()(implicit conn: Connection) = {
    SQL(s"SELECT * FROM $tableName").as(rowParser *)
  }

  def insert(x: A)(implicit conn: Connection): Unit

  def insert(xs: Seq[A])(implicit conn: Connection): Unit = singleCommit {
    for (x <- xs) insert(x)
  }

  def insertMissing(xs: Seq[A])(implicit conn: Connection) = {
    val current = selectAll().toSet // find current systems
    insert(xs.filterNot(current.contains)) // insert the ones which are not in the current list
  }

  def update(x: A)(implicit conn: Connection): Unit

  def update(xs: Seq[A])(implicit conn: Connection): Unit = singleCommit {
    for (x <- xs) update(x)
  }

  def delete(x: A)(implicit conn: Connection): Unit

  def delete(xs: Seq[A])(implicit conn: Connection): Unit = singleCommit {
    for (x <- xs) delete(x)
  }

  protected def singleCommit(code: => Unit)(implicit conn: Connection): Unit = {
    val original = conn.getAutoCommit
    try {
      conn.setAutoCommit(false)
      code
      conn.commit()
    } catch {
      case e: Throwable => conn.rollback(); throw e
    } finally {
      conn.setAutoCommit(original)
    }
  }
}
