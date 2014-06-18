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
    val seqRuns = e.conf.getIntList("experiment.runs").asScala.toList
    logger.info("Starting experiment sequence with " + seqRuns.length + " Experiment(s) and " + seqRuns + " run(s) each")

    // TODO implement update functionalities after sequence is over( e.update())
    // components with lifespan experiment-sequence have to be updated with new parameters

    for (seq <- seqRuns) {
      e.run(ctx.setRuns(seq))
      e.update()
    }
  }

}
