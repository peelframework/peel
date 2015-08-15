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
package org.peelframework.core.beans.system

import java.nio.file.{Files, Path}
import java.security.MessageDigest

import com.typesafe.config.ConfigFactory
import org.peelframework.core.config.Configurable
import org.peelframework.core.graph.Node
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

import scala.concurrent._
import scala.concurrent.duration._

/** Logic to execute a job in a System.
  *
  * This class represents a normal job in a System such as Flink or Spark. It wraps the job-file (that is probably
  * packaged as a .jar file and provides the functionality to run and cancel it, similar to an
  * [[org.peelframework.core.beans.experiment.Experiment Experiment]].
  *
  * @param command Command to run the job (as specified by the specific system).
  * @param runner System that this job is written for.
  * @param timeout Timeout limit for the job. If job does not finish within the given limit, it is canceled.
  * @tparam R Type of the runner system.
  */
abstract class Job[+R <: System](val command: String, val runner: R, timeout: Long) extends Node with Configurable with BeanNameAware {

  import ExecutionContext.Implicits.global
  import scala.language.postfixOps

  final val logger = LoggerFactory.getLogger(this.getClass)

  /** The name of this bean. */
  var beanName = s"${runner.beanName}-job-${MessageDigest.getInstance("MD5").digest(command.getBytes)}"

  /** The home folder for this bean. Depends on the current `beanName` */
  def home = f"${config.getString("app.path.results")}/${config.getString("app.suite.name")}/$beanName"

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

  /** Checks if the given path is a writable folder.
    *
    * If the folder at the given path does not exists, it is created.
    * If it exists but is not a directory or is not writable, this method throws
    * a RuntimeException.
    *
    * @param folder path to the folder
    * @return Unit
    * @throws RuntimeException if folder exists but is not a writable directory
    */
  protected final def ensureFolderIsWritable(folder: Path): Unit = {
    if (Files.exists(folder)) {
      if (!(Files.isDirectory(folder) && Files.isWritable(folder))) throw new RuntimeException(s"Experiment home '$home' is not a writable directory")
    } else {
      Files.createDirectories(folder)
    }
  }

  /** Bean name setter.
    *
    * @param n The configured bean name
    */
  override def setBeanName(n: String) = beanName = n
}

