package org.peelframework.flink.beans.experiment

import java.io.FileWriter
import java.nio.file.{Files, Paths}

import java.lang.{System => Sys}
import com.typesafe.config.Config
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.util.{Version, shell}
import org.peelframework.flink.beans.system.Flink
import spray.json._

/** An [[org.peelframework.core.beans.experiment.Experiment Experiment]] implementation which handles the execution
  * of a single Flink job.
  */
class FlinkExperiment(
    command: String,
    runner : Flink,
    runs   : Int,
    inputs : Set[DataSet],
    outputs: Set[ExperimentOutput],
    name   : String,
    config : Config) extends Experiment(command, runner, runs, inputs, outputs, name, config) {

  def this(
    runs   : Int,
    runner : Flink,
    input  : DataSet,
    output : ExperimentOutput,
    command: String,
    name   : String,
    config : Config) = this(command, runner, runs, Set(input), Set(output), name, config)

  def this(
    runs   : Int,
    runner : Flink,
    inputs : Set[DataSet],
    output : ExperimentOutput,
    command: String,
    name   : String,
    config : Config) = this(command, runner, runs, inputs, Set(output), name, config)

  def this(
    runs: Int,
    runner : Flink,
    input  : DataSet,
    outputs: Set[ExperimentOutput],
    command: String,
    name   : String,
    config : Config) = this(command, runner, runs, Set(input), outputs, name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Flink] = new FlinkExperiment.SingleJobRun(id, this, force)

  def copy(name: String = name, config: Config = config) = new FlinkExperiment(command, runner, runs, inputs, outputs, name, config)
}

object FlinkExperiment {

  case class State(
    name: String,
    suiteName: String,
    command: String,
    runnerID: String,
    runnerName: String,
    runnerVersion: String,
    var runExitCode: Option[Int] = None,
    var runTime: Long = 0,
    var plnExitCode: Option[Int] = None) extends Experiment.RunState {}

  object StateProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val stateFormat = jsonFormat9(State)
  }

  /** A private inner class encapsulating the logic of single run. */
  class SingleJobRun(val id: Int, val exp: FlinkExperiment, val force: Boolean) extends Experiment.SingleJobRun[Flink, State] {

    import org.peelframework.flink.beans.experiment.FlinkExperiment.StateProtocol._

    val runnerLogPath = exp.config.getString("system.flink.path.log")

    override def isSuccessful = state.runExitCode.getOrElse(-1) == 0 // FIXME: && state.plnExitCode.getOrElse(-1) == 0

    override protected def logFilePatterns = List(s"$runnerLogPath/flink-*.log", s"$runnerLogPath/flink-*.out")

    override protected def loadState(): State = {
      if (Files.isRegularFile(Paths.get(s"$home/state.json"))) {
        try {
          scala.io.Source.fromFile(s"$home/state.json").mkString.parseJson.convertTo[State]
        } catch {
          case e: Throwable => State(name, Sys.getProperty("app.suite.name"), command, exp.runner.beanName, exp.runner.name, exp.runner.version)
        }
      } else {
        State(name, Sys.getProperty("app.suite.name"), command, exp.runner.beanName, exp.runner.name, exp.runner.version)
      }
    }

    override protected def writeState() = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }

    override protected def runJob() = {
      // try to get the experiment run plan
      val (plnExit, _) = {
        // assemble options
        val opts = Seq(
          if (Version(exp.runner.version) <= Version("0.8")) Some("-e") else Option.empty[String]
        )
        // execute command
        Experiment.time(this !(s"info ${opts.flatten.mkString(" ")} ${command.trim}", s"$home/run.pln", s"$home/run.pln"))
      }
      state.plnExitCode = Some(plnExit)
      // try to execute the experiment run plan
      val (runExit, t) = Experiment.time(this !(s"run ${command.trim}", s"$home/run.out", s"$home/run.err"))
      state.runTime = t
      state.runExitCode = Some(runExit)
    }

    override def cancelJob() = {
      val ids = (shell !! s"${exp.config.getString("system.flink.path.home")}/bin/flink list -r | tail -n +2 | head -n 1 | cut -d':' -f4 | tr -d ' '").split(Array('\n', ' '))
      for (id <- ids) shell ! s"${exp.config.getString("system.flink.path.home")}/bin/flink cancel -i $id"
      state.runTime = exp.config.getLong("experiment.timeout") * 1000
      state.runExitCode = Some(-1)
    }

    private def !(command: String, outFile: String, errFile: String) = {
      shell ! s"${exp.config.getString("system.flink.path.home")}/bin/flink ${command.trim} > $outFile 2> $errFile"
    }
  }

}
