package eu.stratosphere.peel.core.beans.system

import com.typesafe.config.ConfigFactory
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._

abstract class Job[+R <: System](val command: String, val runner: R, timeout: Long) extends Node with Configurable {

  import ExecutionContext.Implicits.global
  import scala.language.postfixOps

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  def runJob(): Int

  def cancelJob()

  def execute() = {
    logger.info("Running Job with command %s".format(resolve(command)))

    try {
      val exitCode = Await.result(future(runJob()), timeout seconds)
      if (exitCode != 0) throw new RuntimeException("Data generation job did not finish successfully")
    } catch {
      case e: TimeoutException =>
        logger.warn(s"Job did not finish within the given time limit of ${config.getLong("job.timeout")} seconds")
        cancelJob()
        throw e
      case e: InterruptedException =>
        logger.warn(s"Job was interrupted")
        cancelJob()
        throw e
      case e: Throwable =>
        logger.warn(s"Job threw an unexpected exception: ${e.getMessage}")
        cancelJob()
        throw e
    }
  }
}

