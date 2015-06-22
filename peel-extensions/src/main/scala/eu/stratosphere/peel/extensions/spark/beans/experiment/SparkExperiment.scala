package eu.stratosphere.peel.extensions.spark.beans.experiment

import java.io.{FileWriter, IOException}
import java.lang.{System => Sys}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.spark.beans.system.Spark
import spray.json._

/** Experiment-Class for an experiment in Spark.
 *
 * @param command The command that specifies the execution of the experiment in terms of the underlying system's way of
 *                submitting jobs. Example command for a Flink-experiment:
 *
 *                <code>-p 16 ./examples/flink-java-examples-0.7.0-incubating-WordCount.jar
 *                file:///home/user/hamlet.txt file:///home/user/wordcount_out
 *                </code>
 *
 *                You do not have to state the command that is used to 'run' the command (e.g. in Flink
 *                <code> ./bin/flink run </code>
 *
 * @param runner The system that is used to run the experiment (e.g. Flink, Spark, ...)
 * @param runs The number of runs/repetitions of this experiment
 * @param inputs Input Datasets for the experiment
 * @param outputs The output of the Experiment
 * @param name Name of the Experiment
 * @param config Config Object for the experiment
 */
class SparkExperiment(command: String,
                      runner: Spark,
                      runs: Int,
                      inputs: Set[DataSet],
                      outputs: Set[ExperimentOutput],
                      name: String,
                      config: Config) extends Experiment(command, runner, runs, inputs, outputs, name, config) {

  def this(runs: Int, runner: Spark, input: DataSet, output: ExperimentOutput, command: String, name: String, config: Config) = this(command, runner, runs, Set(input), Set(output), name, config)

  def this(runs: Int, runner: Spark, inputs: Set[DataSet], output: ExperimentOutput, command: String, name: String, config: Config) = this(command, runner, runs, inputs, Set(output), name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Spark] = new SparkExperiment.SingleJobRun(id, this, force)
}

object SparkExperiment {

  case class State(name: String,
                   suiteName: String,
                   command: String,
                   runnerID: String,
                   runnerName: String,
                   runnerVersion: String,
                   var runExitCode: Option[Int] = None,
                   var runTime: Long = 0) extends Experiment.RunState {}

  object StateProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val stateFormat = jsonFormat8(State)
  }

  /**
   * A private inner class encapsulating the logic of single run.
   */
  class SingleJobRun(val id: Int, val exp: SparkExperiment, val force: Boolean) extends Experiment.SingleJobRun[Spark, State] {

    import eu.stratosphere.peel.extensions.spark.beans.experiment.SparkExperiment.StateProtocol._

    val runnerLogPath = exp.config.getString("system.spark.path.log")

    override def isSuccessful = state.runExitCode.getOrElse(-1) == 0

    override protected def logFilePatterns = List(s"$runnerLogPath/spark-*.out*")

    override protected def loadState(): State = {
      if (Files.isRegularFile(Paths.get(s"$home/state.json"))) {
        try {
          io.Source.fromFile(s"$home/state.json").mkString.parseJson.convertTo[State]
        } catch {
          case e: Throwable => State(name, Sys.getProperty("app.suite.name"), command, exp.runner.beanName, exp.runner.name, exp.runner.version)
        }
      } else {
        State(name, Sys.getProperty("app.suite.name"), command, exp.runner.beanName, exp.runner.name, exp.runner.version)
      }
    }

    var latestEventLogBeforeRun: Option[String] = None

    val eventLogPattern = s"$runnerLogPath/app-*"

    override protected def beforeRun(): Unit = {
      super.beforeRun()
      try {
        latestEventLogBeforeRun = (for (f <- (shell !! s"ls -t $eventLogPattern").split(Sys.lineSeparator).map(_.trim)) yield f).headOption
      } catch {
        case e: Exception => latestEventLogBeforeRun = None
      }
    }

    override protected def afterRun(): Unit = {
      super.afterRun()
      val eventLog = (for (f <- (shell !! s"ls -t $eventLogPattern").split(Sys.lineSeparator).map(_.trim)) yield f).headOption
      if (eventLog.isEmpty || eventLog == latestEventLogBeforeRun) logger.warn("No event log created for experiment")
      else {
        shell ! s"cp ${eventLog.head} $home/logs/${Paths.get(eventLog.head).getFileName}"
      }
    }

    override protected def writeState() = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }

    override protected def runJob() = {
      // try to execute the experiment run plan
      val (runExit, t) = Experiment.time(this !(s"$command", s"$home/run.out", s"$home/run.err"))
      state.runTime = t
      state.runExitCode = Some(runExit)
    }

    override def cancelJob() = {

    }

    private def !(command: String, outFile: String, errFile: String) = {
      val master = exp.config.getString("system.spark.config.defaults.spark.master")
//      shell ! s"${exp.config.getString("system.spark.path.home")}/bin/spark-submit --master $master $command > $outFile 2> $errFile"
      shell ! s"${exp.config.getString("system.spark.path.home")}/bin/spark-submit $command"
    }
  }

}

