package eu.stratosphere.peel.core.results.etl

import java.io.File

trait EventExtractorFactory {

  type A

  def format(state: RunState, file: File): Option[EventSourceReader[A]] = ???

  def handlers(state: RunState, file: File): Seq[EventExtractor[A]] = ???
}
