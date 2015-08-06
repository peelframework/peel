package eu.stratosphere.peel.core.results.etl.extractor

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import eu.stratosphere.peel.core.results.etl.reader.FileReader
import eu.stratosphere.peel.core.results.model.ExperimentRun
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

  /** A list of file patterns in which the event extractor is interested. */
  val filePatterns: Seq[Regex]

  /** A file can be processed if and only if at least one of the file patterns matches. */
  override final def canProcess(file: File): Boolean = {
    filePatterns.exists(_.pattern.matcher(file.getName).matches())
  }
}
