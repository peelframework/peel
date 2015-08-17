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
package org.peelframework.core.results.etl.extractor

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.peelframework.core.results.etl.reader.FileReader
import org.peelframework.core.results.model.ExperimentRun
import org.springframework.context.ApplicationContext

import scala.collection._
import scala.util.matching.Regex

/** Base trait for all event extractors. */
trait EventExtractor[A] extends Actor with ActorLogging {

  /** Experiment run associated with this extractor. */
  val run: ExperimentRun

  /** The enclosing application context. */
  val appContext: ApplicationContext

  /** The writer actor that loads the extracted events into the database. */
  val writer: ActorRef
}

/** Factory trait for [[EventExtractor]] implementations. */
trait EventExtractorCompanion {

  /** Checks whether extractor produced by this factory can extract events from the specified file. */
  def canProcess(file: File): Boolean

  /** Constructs a reader that parses the file as a sequence of objects that can be handled by the extractor actor. */
  def reader(file: File): FileReader[Any]

  /** Create the extractor props. */
  def props(run: ExperimentRun, context: ApplicationContext, writer: ActorRef): Props
}

/** A trait implementing pattern-based `canProcess` behavior for [[EventExtractorCompanion]] objects. */
trait PatternBasedProcessMatching {
  self: EventExtractorCompanion =>

  /** A prefix for the relative file that needs to match. **/
  val prefix: String

  /** A list of file patterns in which the event extractor is interested. */
  val filePatterns: Seq[Regex]

  /** A file can be processed if and only if at least one of the file patterns matches. */
  override final def canProcess(file: File): Boolean = {
    val preMatches = file.toString.startsWith(prefix)
    val patMatches = filePatterns.exists(_.pattern.matcher(file.getName).matches())
    preMatches && patMatches
  }
}
