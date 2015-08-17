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

import java.io.File
import java.nio.file.Path
import java.sql.Connection

import akka.actor._
import akka.routing.{Broadcast, FromConfig}
import com.typesafe.config.Config
import org.peelframework.core.results.etl.extractor.EventExtractorCompanion
import org.peelframework.core.results.model.ExperimentRun
import org.springframework.context.ApplicationContext

import scala.collection.JavaConverters._
import scala.language.{existentials, postfixOps}

/** EventExtractorManager actor.
  *
  * Handles `ProcessFile` messages.
  */
class EventExtractorManager(appContext: ApplicationContext, config: Config, conn: Connection) extends Actor with ActorLogging {

  import EventExtractorManager._
  import FileProcessor.Process

  /** Writer actor. */
  val writer = context.watch(context.actorOf(Writer.props(appContext, conn), "writer"))

  /** Processor pool actor. */
  val processor = context.watch(context.actorOf(FromConfig.props(FileProcessor.props(appContext)), "processor"))

  /** Registered extractor companion objects. */
  val companions = appContext.getBeansOfType(classOf[EventExtractorCompanion]).asScala.values.toSeq

  override def preStart() = {
    log.info(s"Staring EventExtractorManager")
  }

  override def postStop() = {
    log.info(s"Stopped EventExtractorManager")
  }

  /** Normal state message handler. */
  override def receive: Receive = {
    case process@ProcessFile(basePath, file, run) =>
      // find extractors for this file
      val extractors = for (companion <- companions; if companion.canProcess(process.relativeFile)) yield {
        val p = companion.props(run, appContext, writer) // construct extractor props
        val r = companion.reader(file) // construct reader required for this actor
        r -> p // return (reader, extractor props) pair
      }
      // send process message if at least one extractor exists
      if (extractors.nonEmpty) {
        val noOfRs = extractors.map(_._1).distinct.size
        val noOfEs = extractors.map(_._2).distinct.size
        log.info(s"Processing file '$file' with $noOfEs extractors and $noOfRs readers")

        // process the underlying file for each distinct file reader
        for ((reader, extractors) <- extractors.groupBy(_._1)) {
          processor ! Process(reader, for ((_, props) <- extractors) yield props)
        }
      }
    case Shutdown =>
      processor ! Broadcast(FileProcessor.Shutdown)
      context become shuttingDown
  }

  /** "Shutting Down" state message handler. */
  def shuttingDown: Receive = {
    case ProcessFile(_, _, _) =>
      log.warning("Cannot handle 'ProcessFile' message in EventExtractorManager who is shutting down.")
    case Terminated(actor) if actor == processor =>
      writer ! PoisonPill // all processors are done now, it is safe to send the PoisonPill to the writer
    case Terminated(actor) if actor == writer =>
      context stop self // the writer is the last child to terminate
      context.system.shutdown() // after that we can shutdown the whole ActorSystem
  }
}

/** Companion object. */
object EventExtractorManager {

  /** Used by others to ask to process a file associated with an experiment run. */
  case class ProcessFile(basePath: Path, file: File, run: ExperimentRun) {
    lazy val relativeFile = basePath.relativize(file.toPath).toFile
  }

  /** Shutdown message for FileProcessor actors. */
  case class Shutdown()

  /** Props constructor. */
  def props(context: ApplicationContext, config: Config, conn: Connection): Props = {
    Props(new EventExtractorManager(context, config, conn))
  }
}