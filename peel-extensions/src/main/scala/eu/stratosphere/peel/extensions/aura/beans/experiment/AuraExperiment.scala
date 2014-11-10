package eu.stratosphere.peel.extensions.aura.beans.experiment

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.aura.beans.system.Aura
import spray.json._

class AuraExperiment(command: String,
                     runner: Aura,
                     runs: Int,
                     inputs: Set[DataSet],
                     outputs: Set[ExperimentOutput],
                     name: String,
                     config: Config) extends Experiment(command, runner, runs, inputs, outputs, name, config) {

  def this(runs: Int, runner: Aura, input: DataSet, output: ExperimentOutput, command: String, name: String, config: Config) = this(command, runner, runs, Set(input), Set(output), name, config)

  def this(runs: Int, runner: Aura, inputs: Set[DataSet], output: ExperimentOutput, command: String, name: String, config: Config) = this(command, runner, runs, inputs, Set(output), name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Aura] = new AuraExperiment.SingleJobRun(id, this, force)
}

object AuraExperiment {

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
  class SingleJobRun(val id: Int, val exp: AuraExperiment, val force: Boolean) extends Experiment.SingleJobRun[Aura, State] {

    import eu.stratosphere.peel.extensions.aura.beans.experiment.AuraExperiment.StateProtocol._

    val runnerLogPath = exp.config.getString("system.aura.path.log")

    override def isSuccessful = state.plnExitCode.getOrElse(-1) == 0 && state.runExitCode.getOrElse(-1) == 0

    override protected def logFilePatterns = List(s"$runnerLogPath/aura-*.log", s"$runnerLogPath/aura-*.out")

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
      println("RUN JOB!")
      // try to get the experiment run plan
//      val (plnExit, _) = Experiment.time(this !(s"info -e $command", s"$home/run.pln", s"$home/run.pln"))
//      state.plnExitCode = Some(plnExit)
      // try to execute the experiment run plan
      val (runExit, t) = Experiment.time(this !(s"$command", s"$home/run.out", s"$home/run.err"))
      state.runTime = t
      state.runExitCode = Some(runExit)
    }

    override def cancelJob() = {
      val ids = (shell !! s"${exp.config.getString("system.aura.path.home")}/bin/flink list -r | tail -n +2 | head -n 1 | cut -d':' -f4 | tr -d ' '").split(Array('\n', ' '))
      for (id <- ids) shell ! s"${exp.config.getString("system.aura.path.home")}/bin/flink cancel -i $id"
      state.runTime = exp.config.getLong("experiment.timeout") * 1000
      state.runExitCode = Some(-1)
    }

    private def !(command: String, outFile: String, errFile: String) = {
      shell ! s"${exp.config.getString("system.aura.path.home")}/bin/aura $command > $outFile 2> $errFile"
    }
  }

}
