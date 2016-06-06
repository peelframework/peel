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
package org.peelframework.dstat.results.etl

import java.io.File
import java.nio.file.Paths
import java.time.Instant

import akka.actor.{ActorRef, FSM, Props}
import org.peelframework.core.results.etl.extractor.{EventExtractor, EventExtractorCompanion, PatternBasedProcessMatching}
import org.peelframework.core.results.etl.reader.{FileReader, Line, LineFileReader}
import org.peelframework.core.results.model.{ExperimentEvent, ExperimentRun}
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component

import scala.util.matching.Regex

/** An extractor for Flink task transition events. */
class DstatEventExtractor(
  override final val run: ExperimentRun,
  override final val appContext: ApplicationContext,
  override final val file: File,
  override final val writer: ActorRef) extends EventExtractor[Line] with FSM[State, Data] {

  import DstatEventExtractor.Format._

  override final val companionHash = DstatEventExtractor.hashCode()

  final val logger = LoggerFactory.getLogger(this.getClass)

  final val hostname = DstatEventExtractor.hostname(file)

  startWith(ParseColumnGroups, Uninitialized)

  when(ParseColumnGroups) {

    case Event(Line(""), _) =>
      logger.debug(s"Ignoring empty line at $stateName")
      stay

    case Event(Line(Title(_)), _) =>
      logger.debug(s"Ignoring Title line at $stateName")
      stay

    case Event(Line(AuthorInfo(_, _)), _) =>
      logger.debug(s"Ignoring AuthorInfo line at $stateName")
      stay

    case Event(Line(MachineInfo(_, _)), _) =>
      logger.debug(s"Ignoring MachineInfo line at $stateName")
      stay

    case Event(Line(CommandInfo(_, _)), _) =>
      logger.debug(s"Ignoring CommandInfo line at $stateName")
      stay

    case Event(Line(ColumnSeq(groups@_*)), _) =>
      logger.debug("Transitioning to ParseColumns")
      goto(ParseColumns) using ColumnGroups(groups)
  }

  when(ParseColumns) {

    case Event(Line(""), _) =>
      logger.debug(s"Ignoring empty line at $stateName")
      stay

    case Event(Line(ColumnSeq(cols@_*)), ColumnGroups(groups)) =>
      logger.debug("Transitioning to ParseData")
      goto(ParseData) using Columns(for ((g, c) <- groups zip cols) yield if (g != c) s"$g:$c" else g)
  }

  when(ParseData) {

    case Event(Line(""), _) =>
      logger.debug(s"Ignoring empty line at $stateName")
      stay

    case Event(Line(DataSeq(vals@_*)), Columns(cols)) =>
      val pairs = (cols zip vals).toMap

      val epochOpt = pairs("epoch").split('.').toList match {
        case sec :: milli :: Nil => Some(Instant.ofEpochMilli(sec.toLong * 1000 + milli.toShort))
        case _ => None
      }

      for {
        (c, v) <- pairs if c != "epoch"
        epoch <- epochOpt
      } writer ! ExperimentEvent.apply(
          id = nextID(),
          experimentRunID = run.id,
          name = Symbol(s"dstat_$c"),
          host = Some(hostname),
          vDouble = Some(v.toDouble),
          vTimestamp = Some(epoch))

      stay
  }

  whenUnhandled {
    case Event(e, s) =>
      log.warning(s"received unhandled request $e in state $stateName/$s")
      stay
  }

  initialize()
}

/** Companion object. */
@Component
object DstatEventExtractor extends EventExtractorCompanion with PatternBasedProcessMatching {

  /** A prefix fore the relative file that needs to match. **/
  override val prefix: String = {
    Paths.get("logs", "dstat").toString
  }

  val filePattern = "dstat-(\\w+)-(.+)\\.csv".r

  /** A list of file patterns for in which the event extractor is interested */
  override val filePatterns: Seq[Regex] = {
    Seq(filePattern)
  }

  /** Constructs a reader that parses the file as a sequence of objects that can be handled by the extractor actor. */
  override def reader(file: File): FileReader[Any] = {
    LineFileReader(file)
  }

  /** Create the extractor props. */
  override def props(run: ExperimentRun, context: ApplicationContext, file: File, writer: ActorRef): Props = {
    Props(new DstatEventExtractor(run, context, file, writer))
  }

  private[etl] def hostname(file: File) = file.getName match {
    case filePattern(_, hostname) => hostname
  }

  private[etl] object Format {

    /** Pattern for job output log entries. */
    val Title =
      """ "Dstat ([0-9\.]+) CSV output" """.trim.r

    /** Pattern for job output log entries. */
    val AuthorInfo =
      """ "Author:","(.+)",,,,"URL:","(.+)"  """.trim.r

    /** Pattern for task state transitions. */
    val MachineInfo =
      """ "Host:","(.+)",,,,"User:","(.+)" """.trim.r

    /** Pattern for task state transitions. */
    val CommandInfo =
      """ "Cmdline:","(.+)",,,,"Date:","(.+)" """.trim.r
  }

}

// states
private[etl] sealed trait State

private[etl] case object ParseColumnGroups extends State

private[etl] case object ParseColumns extends State

private[etl] case object ParseData extends State

private[etl] sealed trait Data

private[etl] case object Uninitialized extends Data

private[etl] final case class ColumnGroups(groups: Seq[String]) extends Data

private[etl] final case class Columns(cols: Seq[String]) extends Data

private[etl] object ColumnSeq {
  def unapplySeq(str: String): Option[Seq[String]] = Some {
    val names = for (s <- str.split(',')) yield s
      .stripPrefix("\"") // trim the opening quote
      .stripSuffix("\"") // trim the closing quote
      .replaceAll("\\W", "_") // remove non-word characters

    for {
      (name, i) <- names.zipWithIndex // iterate over column names
      j <- (i to 0 by -1).find(names(_).nonEmpty) // find nearest previous non-empty name
    } yield names(j)
  }
}

private[etl] object DataSeq {
  def unapplySeq(str: String): Option[Seq[String]] = Some(str.split(","))
}
