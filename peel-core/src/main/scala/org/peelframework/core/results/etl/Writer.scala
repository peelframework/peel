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

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.sql.Connection
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import akka.actor._
import org.apache.commons.lang3.StringEscapeUtils._
import org.peelframework.core.results.model.ExperimentEvent
import org.springframework.context.ApplicationContext

import scala.language.{existentials, postfixOps}

/** Writer actor.
  *
  * Writes [[org.peelframework.core.results.model.ExperimentEvent ExperimentEvent]] instances to the database.
  */
class Writer(appContext: ApplicationContext, conn: Connection) extends Actor with ActorLogging {

  import Writer._

  /** Keep track of what we're watching. */
  private var count = 0

  private val file = Files.createTempFile("peel-experiment-events-", ".csv", Permissions)
  private val out = new BufferedWriter(new FileWriter(file.toString, true), BufferSize)

  override def preStart(): Unit = {
    log.info(s"Starting Writer")
    log.info(s"Accumulating extracted events into '${file.toAbsolutePath}'")
  }

  override def postStop(): Unit = {
    close()
    log.info(s"Extracted $count events into '${file.toAbsolutePath}'")

    ExperimentEvent.importCSV(file, Fsep, Lsep, Quote, Null)(conn)
    log.info(s"Imported CSV data into the database")

    log.info(s"Stopped Writer")
  }

  /** Normal state message handler. */
  override def receive: Receive = {
    case event: ExperimentEvent =>
      // serialize the event as a byte array
      write(event)
      count += 1
  }

  private def write(event: ExperimentEvent) = {
    // id
    out.write(event.id.toString)
    out.write(Writer.Fsep)
    // experimentRunID
    out.write(event.experimentRunID.toString)
    out.write(Writer.Fsep)
    // name
    out.write(escapeCsv(event.name.name))
    out.write(Writer.Fsep)
    // host
    event.host match {
      case Some(v) => out.write(escapeCsv(v.toString))
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Fsep)
    // task
    event.task match {
      case Some(v) => out.write(escapeCsv(v.toString))
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Fsep)
    // taskInstance
    event.taskInstance match {
      case Some(v) => out.write(v.toString)
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Fsep)
    // vLong
    event.vLong match {
      case Some(v) => out.write(v.toString)
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Fsep)
    // vDouble
    event.vDouble match {
      case Some(v) => out.write(v.toString)
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Fsep)
    // vTimestamp
    event.vTimestamp match {
      case Some(v) => out.write(escapeCsv(TimestampFmt.format(v)))
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Fsep)
    // vString
    event.vString match {
      case Some(v) => out.write(escapeCsv(v))
      case None => out.write(Writer.Null)
    }
    out.write(Writer.Lsep)
  }

  private def close(): Unit = {
    out.flush()
    out.close()
  }
}

/** Companion object. */
object Writer {

  val Permissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr--r--"))

  val BufferSize = 1024 * 1024 * 32
  private val TimestampFmt = DateTimeFormatter
    .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    .withZone(ZoneId.systemDefault())

  val Quote = '"'
  val Fsep = ','
  val Lsep = '\n'
  val Null = ""

  /** Props constructor. */
  def props(context: ApplicationContext, conn: Connection): Props = {
    Props(new Writer(context, conn))
  }
}
