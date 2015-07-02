package eu.stratosphere.peel.core.beans.system

import com.typesafe.config.ConfigFactory
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._

/** Logic to execute a job in a System.
  *
  * This class represents a normal job in a System such as Flink or Spark. It wraps the job-file (that is probably
  * packaged as a .jar file and provides the functionality to run and cancel it, similar to an
  * [[eu/stratosphere/peel/core/beans/experiment/Experiment.scala Experiment]].
 *
 * @param command Command to run the job (as specified by the specific system)
 * @param runner System that this job is written for
 * @param timeout Timeout limit for the job. If job does not finish within the given limit, it is canceled
 * @tparam R Type of the system/runner
 */
abstract class Job[+R <: System](val command: String, val runner: R, timeout: Long) extends Node with Configurable {

  import ExecutionContext.Implicits.global
  import scala.language.postfixOps

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  /** Runs the job on specified runner */
  def runJob(): Int

  /** Cancels the job */
  def cancelJob()

  /** Executes job and handles exceptions and timeouts. */
  def execute() = {
    logger.info("Running Job with command %s".format(resolve(command)))

    try {
      val exitCode = Await.result(future(runJob()), timeout seconds)
      if (exitCode != 0) throw new RuntimeException("Data generation job did not finish successfully")
    } catch {
      case e: TimeoutException =>
        logger.warn(s"Job did not finish within the given time limit of $timeout seconds")
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

