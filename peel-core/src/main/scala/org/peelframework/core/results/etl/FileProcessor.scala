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
package org.peelframework.core.results.etl

import akka.actor._
import org.peelframework.core.results.etl.reader.FileReader
import org.springframework.context.ApplicationContext

import scala.collection.mutable.ArrayBuffer
import scala.language.{existentials, postfixOps}

/** FileProcessor actor. Handles `Process` messages. */
class FileProcessor(appContext: ApplicationContext) extends Actor with ActorLogging {

  import FileProcessor.{Process, Shutdown}

  /** Keep track of what we're watching. */
  val watched = ArrayBuffer.empty[ActorRef]

  override def preStart() = {
    log.info(s"Staring FileProcessor")
  }

  override def postStop() = {
    log.info(s"Stopped FileProcessor")
  }

  /** Normal state message handler. */
  override def receive: Receive = {
    case process@Process(reader, extractorProps) =>
      // instantiate extractors
      val extractors = for (props <- extractorProps) yield {
        context.actorOf(props)
      }
      // watch instantiated extractors
      watched ++= (for (ext <- extractors) yield {
        context.watch(ext)
      })
      // traverse the reader and send each parsed message to all extractors
      for (msg <- reader; ext <- extractors) {
        ext ! msg
      }
      // shutdown extractors
      for (ext <- extractors) {
        ext ! PoisonPill
      }
    case Terminated(extractor) =>
      watched -= extractor
    case Shutdown =>
      if (watched.isEmpty) context stop self else context become shuttingDown
  }

  /** "Shutting Down" state message handler. */
  def shuttingDown: Receive = {
    case Process(_, _) =>
      log.warning("Cannot handle 'Process' message in FileProcessor who is shutting down.")
    case Terminated(extractor) =>
      watched -= extractor
      if (watched.isEmpty) context stop self
  }
}

/** Companion object. */
object FileProcessor {

  /** Used by others to ask to process a file associated with an experiment run. */
  case class Process(reader: FileReader[Any], extractorProps: Seq[Props])

  /** Shutdown message for FileProcessor actors. */
  case class Shutdown()

  /** Props constructor. */
  def props(context: ApplicationContext): Props = {
    Props(new FileProcessor(context))
  }
}
