package eu.stratosphere.peel.core.results.etl

import eu.stratosphere.peel.core.results.model.ExperimentEvent

trait EventExtractor[A] {

  def extract(element: A) = ???

  def collect(): Seq[ExperimentEvent] = ???
}
