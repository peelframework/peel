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
package org.peelframework.flink.results.etl

import java.io.File
import java.nio.file.Paths

import akka.actor.{ActorRef, Props}
import org.peelframework.core.results.etl.extractor.{EventExtractor, EventExtractorCompanion, PatternBasedProcessMatching}
import org.peelframework.core.results.etl.reader.{FileReader, Line, LineFileReader}
import org.peelframework.core.results.model.{ExperimentEvent, ExperimentRun}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component

import scala.util.matching.Regex

/** An extractor for Flink task transition events. */
class FlinkTaskEventExtractor(
  override val run: ExperimentRun,
  override val appContext: ApplicationContext,
  override val writer: ActorRef) extends EventExtractor[Line] {

  /** Extracts events from an incoming message */
  final def receive: Receive = {
    case msg@Line(LogEntryV1(time, TaskStateV1(name, number, total, state))) =>
      writer ! ExperimentEvent(
        experimentRunID = run.id,
        name            = Symbol(s"state_change_${state.toLowerCase}"),
        task            = Some(name),
        taskInstance    = Some(number.toInt),
        vTimestamp      = Some(toInstant(time)))
    case msg@Line(LogEntryV2(time, TaskStateV2(name, number, total, state))) =>
      writer ! ExperimentEvent(
        experimentRunID = run.id,
        name            = Symbol(s"state_change_${state.toLowerCase}"),
        task            = Some(name),
        taskInstance    = Some(number.toInt),
        vTimestamp      = Some(toInstant(time)))
  }
}

/** Companion object. */
@Component
object FlinkTaskEventExtractor extends EventExtractorCompanion with PatternBasedProcessMatching {

  /** A prefix fore the relative file that needs to match. **/
  override val prefix: String = {
    Paths.get("logs", "flink").toString
  }

  /** A list of file patterns for in which the event extractor is interested */
  override val filePatterns: Seq[Regex] = {
    Seq("flink-.+-jobmanager-.+\\.log".r)
  }

  /** Constructs a reader that parses the file as a sequence of objects that can be handled by the extractor actor. */
  override def reader(file: File): FileReader[Any] = {
    LineFileReader(file)
  }

  /** Create the extractor props. */
  override def props(run: ExperimentRun, context: ApplicationContext, file: File, writer: ActorRef): Props = {
    Props(new FlinkTaskEventExtractor(run, context, writer))
  }
}
