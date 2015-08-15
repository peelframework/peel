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
package org.peelframework.core.results.etl

import java.sql.Connection

import akka.actor._
import org.peelframework.core.results.model.ExperimentEvent
import org.springframework.context.ApplicationContext

import scala.collection.mutable.ArrayBuffer
import scala.language.{existentials, postfixOps}

/** Writer actor.
  *
  * Writes [[org.peelframework.core.results.model.ExperimentEvent ExperimentEvent]] instances to the database.
  */
class Writer(appContext: ApplicationContext, conn: Connection) extends Actor with ActorLogging {

  import Writer.BatchSize

  /** Keep track of what we're watching. */
  val batch = ArrayBuffer.empty[ExperimentEvent]
  var count = 0

  override def preStart() = {
    log.info(s"Staring Writer")
  }

  override def postStop() = {
    if (batch.nonEmpty) insert()
    log.info(s"Extracted $count events")
    log.info(s"Stopped Writer")
  }

  /** Normal state message handler. */
  override def receive: Receive = {
    case event: ExperimentEvent =>
      count += 1
      batch += event
      if (batch.size >= BatchSize) insert()
  }

  def insert() = {
    try {
      ExperimentEvent.insert(batch.result())(conn)
    } catch {
      case e: Throwable => log.warning(s"SQL Exception on batch insert of ExperimentEvent objects: $e")
    } finally {
      batch.clear()
    }
  }
}

/** Companion object. */
object Writer {

  val BatchSize = 1000

  /** Props constructor. */
  def props(context: ApplicationContext, conn: Connection): Props = {
    Props(new Writer(context, conn: Connection))
  }
}
