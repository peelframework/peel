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
package org.peelframework.hadoop.beans.experiment

import java.io.FileWriter
import java.nio.file.{Paths, Files}

import com.typesafe.config.Config
import org.peelframework.core.beans.data.{ExperimentOutput, DataSet}
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.System
import org.peelframework.core.util.shell
import org.peelframework.hadoop.beans.experiment.YarnExperiment.SingleJobRun
import org.peelframework.hadoop.beans.system.Yarn
import spray.json._

/** An `Expriment` implementation which handles the execution of a single Yarn job. */
class YarnExperiment(command: String,
                     systems: Set[System],
                     runner: Yarn,
                     runs: Int,
                     inputs: Set[DataSet],
                     outputs: Set[ExperimentOutput],
                     name: String,
                     config: Config) extends Experiment(command, systems, runner, runs, inputs, outputs, name, config) {

  def this(
    command: String,
    runner : Yarn,
    runs   : Int,
    inputs : Set[DataSet],
    outputs: Set[ExperimentOutput],
    name   : String,
    config : Config) = this(command, Set.empty[System], runner, runs, inputs, outputs, name, config)
  
  /** Experiment run factory method.
    *
    * @param id The `id` for the constructed experiment run
    * @param force Force execution of this run
    * @return An run for this experiment identified by the given `id`
    */
  override def run(id: Int, force: Boolean): Run[Yarn] = new SingleJobRun(id, this, force)

  /** Copy the object with updated name and config values.
    *
    * @param name The updated name value.
    * @param config The updated config value.
    */
  override def copy(name: String, config: Config): Experiment[Yarn] =
    new YarnExperiment(command, systems, runner, runs, inputs, outputs, name, config)
}

object YarnExperiment {

  case class YarnState(override val runnerID: String,
                       override val runnerName: String,
                       override val runnerVersion: String,
                       override var runTime: Long = 0,
                       override var runExitCode: Option[Int] = None) extends Experiment.RunState

  object YarnStateProtocal extends DefaultJsonProtocol with NullOptions {
    implicit val yarnStateFormat = jsonFormat5(YarnState)
  }

  class SingleJobRun(override val id: Int,
                     override val exp: YarnExperiment,
                     override val force: Boolean) extends Experiment.SingleJobRun[Yarn, YarnState] {

    import YarnStateProtocal.yarnStateFormat

    override protected def loadState(): YarnState = {
      if (Files.isRegularFile(Paths.get(s"$home/state.json"))) {
        try {
          scala.io.Source.fromFile(s"$home/state.json").mkString.parseJson.convertTo[YarnState]
        } catch {
          case e: Throwable => YarnState(exp.runner.beanName, exp.runner.name, exp.runner.version)
        }
      } else {
        YarnState(exp.runner.beanName, exp.runner.name, exp.runner.version)
      }
    }

    override protected def writeState(): Unit = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }

    override protected def runJob(): Unit = {
      val (exitCode, runTime) = Experiment.time {
        val yarnHomeDir = exp.config.getString(s"system.${exp.runner.configKey}.path.home")
        shell ! s"$yarnHomeDir/bin/yarn jar ${command.trim} > $home/run.out 2> $home/run.err"
      }
      state.runExitCode = Some(exitCode)
      state.runTime = runTime
    }

    override protected def cancelJob(): Unit = {} // NOP - do nothing

    /** Check if the execution of this run exited successfully. */
    override def isSuccessful: Boolean = state.runExitCode.getOrElse(-1) == 0
  }

}
