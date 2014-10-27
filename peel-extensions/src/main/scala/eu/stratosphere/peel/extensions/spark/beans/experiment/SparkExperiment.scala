package eu.stratosphere.peel.extensions.spark.beans.experiment

import java.io.{IOException, FileWriter}
import java.nio.file._

import java.lang.{System => Sys}
import java.nio.file.attribute.BasicFileAttributes
import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.spark.beans.system.Spark
import spray.json._

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
                   command: String,
                   var runExitCode: Option[Int] = None,
                   var runTime: Long = 0) extends Experiment.RunState {}

  object StateProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val stateFormat = jsonFormat4(State)
  }

  /**
   * A private inner class encapsulating the logic of single run.
   */
  class SingleJobRun(val id: Int, val exp: SparkExperiment, val force: Boolean) extends Experiment.SingleJobRun[Spark, State] {

    import eu.stratosphere.peel.extensions.spark.beans.experiment.SparkExperiment.StateProtocol._

    val runnerLogPath = exp.config.getString("system.spark.path.log")

    override def isSuccessful = state.runExitCode.getOrElse(-1) == 0

    override protected def logFilePatterns = List(s"$runnerLogPath/*/")

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

    var latestFolderBeforeRun: Option[String] = None

    override protected def beforeRun(): Unit = {
      // get current latest folder
      latestFolderBeforeRun = (for (pattern <- logFilePatterns; f <- (shell !! s"ls -tc $pattern").split(Sys.lineSeparator)) yield f).headOption
    }

    override protected def afterRun(): Unit = {
      try {
        val logFolder = (for (pattern <- logFilePatterns; f <- (shell !! s"ls -tc $pattern").split(Sys.lineSeparator)) yield f).headOption
        logFolder match {
          case Some(dir) => {
            // check if folder is actually new
            latestFolderBeforeRun match {
              case Some(pathBeforeRun) => assert(dir != pathBeforeRun, s"No new event log created, got $dir")
              case None => // experiment is first result
            }

            val path = Paths.get(dir.substring(0, dir.length - 1))

            shell ! s"rm -Rf $home/logs/*"
            val logPath = Paths.get(s"$home/logs/")
            if (!Files.exists(logPath)) {
              Files.createDirectory(logPath)
            }

            Files.walkFileTree(path, new FileVisitor[Path] {
              override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
              override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                Files.copy(file, logPath.resolve(file.getFileName))
                FileVisitResult.CONTINUE
              }
              override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = FileVisitResult.CONTINUE
              override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = FileVisitResult.CONTINUE
            })
          }
          case None => throw new IllegalArgumentException("No event log folder found.")
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }

      // copy logs

//      for ((file, count) <- logFileCounts) shell ! s"tail -n +${count + 1} $file > $home/logs/${Paths.get(file).getFileName}"
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
      shell ! s"${exp.config.getString("system.spark.path.home")}/bin/spark-submit --master $master $command > $outFile 2> $errFile"
    }
  }

}

