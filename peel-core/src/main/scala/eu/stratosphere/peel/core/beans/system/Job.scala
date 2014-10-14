package eu.stratosphere.peel.core.beans.system

import com.typesafe.config.Config
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._



abstract class Job[+R <: System](val command: String, val runner: R, var config: Config) extends Node with Configurable {
  final val logger = LoggerFactory.getLogger(this.getClass)

  def runJob()

  def cancelJob()

  def execute() = {
    logger.info("Running Job with command %s".format(command))

    try {
      Await.ready(future(runJob()), config.getLong("job.timeout") seconds)
    } catch {
      case e: TimeoutException =>
        logger.warn(s"Job did not finish within the given time limit of ${config.getLong("job.timeout")} seconds")
        cancelJob()
      case e: InterruptedException =>
        logger.warn(s"Job was interrupted")
        cancelJob()
      case e: Throwable =>
        logger.warn(s"Job threw an unexpected exception: ${e.getMessage}")
        cancelJob()
    }


  }

}

