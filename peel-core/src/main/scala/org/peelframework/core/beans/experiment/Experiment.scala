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
package org.peelframework.core.beans.experiment

import java.lang.{System => Sys}
import java.nio.file.Paths

import com.typesafe.config.Config
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.system
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.Configurable
import org.peelframework.core.graph.Node
import org.peelframework.core.util.console._
import org.peelframework.core.util.shell
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/** Abstract representation of an experiment.
  *
  * @param command The command that specifies the execution of the experiment in terms of the underlying system's way of
  *                submitting apps. Example command for a Flink-experiment:
  *
  *                <code>-p 16 ./examples/flink-java-examples-0.7.0-incubating-WordCount.jar
  *                file:///home/user/hamlet.txt file:///home/user/wordcount_out
  *                </code>
  *
  *                You do not have to state the command that is used to 'run' the command (e.g. in Flink
  *                <code> ./bin/flink run </code>
  *
  * @param systems Systems that are required for the experiment (excluding the runner).
  * @param runner The system that is used to run the experiment (e.g. Flink, Spark, ...)
  * @param runs The number of runs/repetitions of this experiment
  * @param inputs Input Datasets for the experiment
  * @param outputs The output of the Experiment
  * @param name Name of the Experiment
  * @param config Config Object for the experiment
  * @tparam R The system that is used to execute the experiment
  */
abstract class Experiment[+R <: System](
  val command : String,
  val systems : Set[System],
  val runner  : R,
  val runs    : Int,
  val inputs  : Set[DataSet],
  val outputs : Set[ExperimentOutput],
  val name    : String,
  var config  : Config) extends Node with Configurable {

  /** Experiment run factory method.
    *
    * @param id The `id` for the constructed experiment run
    * @param force Force execution of this run
    * @return An run for this experiment identified by the given `id`
    */
  def run(id: Int, force: Boolean): Experiment.Run[R]

  /** Alias of name.
    *
    * @return name of the Experiment
    */
  override def toString: String = name

  /** Copy the object with updated name and config values.
    *
    * @param name The updated name value.
    * @param config The updated config value.
    */
  def copy(name: String = name, config: Config = config): Experiment[R]
}

/** Object that holds Experiment run properties and utilities. */
object Experiment {

  /** A base trait for experiment runs.
    *
    * @tparam R The type of the associated runner system.
    */
  trait Run[+R <: system.System] {

    final val logger = LoggerFactory.getLogger(this.getClass)

    val id: Int
    val exp: Experiment[R]
    val force: Boolean

    val name = RunName(exp.name, id)
    val home = f"${exp.config.getString("app.path.results")}/${exp.config.getString("app.suite.name")}/$name"

    // ensure experiment folder structure in the constructor
    {
      shell.ensureFolderIsWritable(Paths.get(s"$home"))
      shell.ensureFolderIsWritable(Paths.get(s"$home/logs"))
    }

    /** Check if the execution of this run exited successfully. */
    def isSuccessful: Boolean

    /** Execute the experiment run. */
    def execute(): Unit
  }

  /* Encoding of the experiment run name */
  object RunName {
    private val format = """(.+)\.run(\d{2})""".r

    def apply(expName: String, runNo: Int): String = f"$expName.run$runNo%02d"

    def unapply(runName: String): Option[(String, Int)] = runName match {
      case format(expName, runNo) => Some(expName, runNo.toInt)
      case _ => None
    }
  }

  /** Representation of the state of a run. */
  trait RunState {
    val name          : String
    val runnerID      : String
    val runnerName    : String
    val runnerVersion : String
    var runExitCode   : Option[Int]
    var runTime       : Long
  }

  /** A private inner class encapsulating the logic of single run. */
  trait SingleJobRun[+R <: system.System, RS <: RunState] extends Run[R] {

    var state = loadState()

    /** Executes this run.
      *
      * Tries to execute the specified experiment-job. If the experiment did not finished within the given timelimit
      * (specified by experiment.timeout property in experiment-configuration), the job is canceled. The same happens
      * if the experiment was interrupted or throws an exception.
      */
    override def execute() = {
      if (!force && isSuccessful) {
        logger.info("Skipping successfully finished experiment run %s".format(name).yellow)
      } else {
        logger.info("Running experiment %s".format(name))
        logger.info("Experiment data will be written to %s".format(home))
        logger.info("Experiment command is %s".format(command))

        try {

          for (s <- Set(exp.runner) ++ exp.systems) {
            s.beforeRun(this)
          }

          try {
            Await.ready(future(runJob()), exp.config.getLong("experiment.timeout") seconds)
          } catch {
            case e: TimeoutException =>
              logger.warn(s"Experiment run did not finish within the given time limit of ${exp.config.getLong("experiment.timeout")} seconds")
              cancelJob()
            case e: InterruptedException =>
              logger.warn(s"Experiment run was interrupted")
              cancelJob()
            case e: Throwable =>
              logger.warn(s"Experiment run threw an unexpected exception: ${e.getMessage}")
              cancelJob()
          }

          for (s <- Set(exp.runner) ++ exp.systems) {
            s.afterRun(this)
          }

          if (isSuccessful)
            logger.info(s"Experiment run finished in ${state.runTime} milliseconds")
          else
            logger.warn(s"Experiment run did not finish successfully".yellow)
        } catch {
          case e: Exception =>
            logger.error("Exception in experiment run %s: %s".format(name, e.getMessage).red)
        } finally {
          writeState()
        }
      }
    }

    protected def command: String = exp.resolve(exp.command)

    protected def loadState(): RS

    protected def writeState(): Unit

    protected def runJob(): Unit

    protected def cancelJob(): Unit
  }

  /** Measures the time to execute the blocking function in ms
    *
    * @param block function to be measured
    * @tparam T type of result value of the blocking function
    * @return tuple of result value and measured time
    */
  def time[T](block: => T): (T, Long) = {
    val t0 = Sys.currentTimeMillis
    val result = block
    val t1 = Sys.currentTimeMillis
    (result, t1 - t0)
  }
}
