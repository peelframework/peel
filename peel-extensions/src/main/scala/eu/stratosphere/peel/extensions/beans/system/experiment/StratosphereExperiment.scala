package eu.stratosphere.peel.extensions.beans.system.experiment

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.beans.system.stratosphere.Stratosphere
import spray.json._

class StratosphereExperiment(command: String,
                             runner: Stratosphere,
                             runs: Int,
                             inputs: Set[DataSet],
                             outputs: Set[ExperimentOutput],
                             name: String,
                             config: Config) extends Experiment(command, runner, runs, inputs, outputs, name, config) {

  def this(runs: Int, runner: Stratosphere, input: DataSet, output: ExperimentOutput, command: String, name: String, config: Config) = this(command, runner, runs, Set(input), Set(output), name, config)

  def this(runs: Int, runner: Stratosphere, inputs: Set[DataSet], output: ExperimentOutput, command: String, name: String, config: Config) = this(command, runner, runs, inputs, Set(output), name, config)

  override def run(id: Int): Experiment.Run[Stratosphere] = new StratosphereExperiment.SingleJobRun(id, this)
}

object StratosphereExperiment {

  case class State(name: String,
                   command: String,
                   var runExitCode: Option[Int] = None,
                   var runTime: Long = 0,
                   var plnExitCode: Option[Int] = None) extends Experiment.RunState {}

  object StateProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val stateFormat = jsonFormat5(State)
  }

  /**
   * A private inner class encapsulating the logic of single run.
   */
  class SingleJobRun(val id: Int, val exp: StratosphereExperiment) extends Experiment.SingleJobRun[Stratosphere, State] {

    import eu.stratosphere.peel.extensions.beans.system.experiment.StratosphereExperiment.StateProtocol._

    val runnerLogPath = exp.config.getString("system.stratosphere.path.log")

    override def isSuccessful = state.plnExitCode.getOrElse(-1) == 0 && state.runExitCode.getOrElse(-1) == 0

    override protected def logFilePatterns = List(s"$runnerLogPath/stratosphere-*.log", s"$runnerLogPath/stratosphere-*.out")

    override protected def loadState(): State = {
      if (Files.isRegularFile(Paths.get(s"$home/state.json"))) {
        try {
          io.Source.fromFile(s"$home/state.json").mkString.parseJson.convertTo[State]
        } catch {
          case e: Throwable => State(name, command)
        }
      } else {
        State(name, command)
      }
    }

    override protected def writeState() = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }

    override protected def runJob() = {
      // try to get the experiment run plan
      val (plnExit, _) = Experiment.time(this !(s"info -e $command", s"$home/run.pln", s"$home/run.pln"))
      state.plnExitCode = Some(plnExit)
      // try to execute the experiment run plan
      val (runExit, t) = Experiment.time(this !(s"run $command", s"$home/run.out", s"$home/run.err"))
      state.runTime = t
      state.runExitCode = Some(runExit)
    }

    override def cancelJob() = {
      val ids = (shell !! s"${exp.config.getString("system.stratosphere.path.home")}/bin/stratosphere list -r | tail -n +2 | head -n 1 | cut -d':' -f4 | tr -d ' '").split(Array('\n', ' '))
      for (id <- ids) shell ! s"${exp.config.getString("system.stratosphere.path.home")}/bin/stratosphere cancel -i $id"
      state.runTime = exp.config.getLong("experiment.timeout") * 1000
      state.runExitCode = Some(-1)
    }

    private def !(command: String, outFile: String, errFile: String) = {
      shell ! s"${exp.config.getString("system.stratosphere.path.home")}/bin/stratosphere $command > $outFile 2> $errFile"
    }
  }

}
