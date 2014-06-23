package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.ExecutionContext
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory

/**
 * Created by felix on 16.06.14.
 */
class ExperimentSequence(val e: Experiment) {
  final val logger = LoggerFactory.getLogger(this.getClass)

  def run(ctx: ExecutionContext) = {

  }

}
