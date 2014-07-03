package eu.stratosphere.peel.extensions.beans.system.experiment

import java.io.FileWriter
import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.extensions.beans.system.stratosphere.Stratosphere
import org.slf4j.LoggerFactory
import spray.json._
import eu.stratosphere.peel.core.util.shell

class StratosphereExperiment(val command: String, runner: Stratosphere, runs: Int, inputs: Set[DataSet], outputs: Set[ExperimentOutput], config: Config) extends Experiment(runner, runs, inputs, outputs, config) {

  def this(runs: Int, runner: Stratosphere, input: DataSet, output: ExperimentOutput, command: String, config: Config) = this(command, runner, runs, Set(input), Set(output), config)

  def this(runs: Int, runner: Stratosphere, inputs: Set[DataSet], output: ExperimentOutput, command: String, config: Config) = this(command, runner, runs, inputs, Set(output), config)

  def run(id: Int) = new StratosphereExperiment.Run(id, runner, runner.resolve(command)).execute()
}

object StratosphereExperiment {

  case class State(name: String, command: String, var exit: Option[Int] = None, var time: Long = 0) {}

  object StateProtocol extends DefaultJsonProtocol with NullOptions {
    implicit val stateFormat = jsonFormat4(State)
  }

  /**
   * A private inner class encapsulating the logic of single run.
   */
  private class Run(val id: Int, val runner: Stratosphere, val command: String) {

    import eu.stratosphere.peel.extensions.beans.system.experiment.StratosphereExperiment.StateProtocol._

    final val logger = LoggerFactory.getLogger(this.getClass)

    val home = f"${runner.config.getString("app.path.results")}/${runner.config.getString("app.suite")}/${runner.config.getString("experiment.name")}.run$id%02d"
    val name = f"${runner.config.getString("experiment.name")}.run$id%02d"
    val runnerLogPath = runner.config.getString("system.stratosphere.path.log")

    var state = loadState()

    // ensure experiment folder structure in the constructor
    {
      ensureFolderIsWritable(Paths.get(s"$home"))
      ensureFolderIsWritable(Paths.get(s"$home/stratosphere-logs"))

      loadState()
    }

    def execute() = {
      if (state.exit.getOrElse(-1) == 0) {
        logger.info("Skipping successfully executed experiment %s".format(name))
      } else {
        logger.info("Running experiment %s".format(name))
        logger.info("Experiment data will be written to %s".format(home))
        logger.info("Experiment command is %s".format(command))

        try {
          // try to get the experiment run plan
          val (plnExit, _) = time(runner.run(s"info -e $command", s"$home/run.pln", s"$home/run.pln"))
          if (plnExit != 0) throw new RuntimeException(s"Experiment plan info command exited with non-zero code")

          // collect current number of lines in log and out files
          var logFiles = collection.mutable.Map[String, Long]()
          for (f <- (shell !! s"ls $runnerLogPath/stratosphere-*.log").split(System.lineSeparator).map(_.trim)) {
            logFiles += f -> (shell !! s"wc -l $f | cut -d' ' -f1").trim.toLong
          }
          for (f <- (shell !! s"ls $runnerLogPath/stratosphere-*.out").split(System.lineSeparator).map(_.trim)) {
            val x = f
            logFiles += f -> (shell !! s"wc -l $f | cut -d' ' -f1").trim.toLong
          }

          // try to execute the experiment run plan
          val (runExit, t) = time(runner.run(s"run $command", s"$home/run.out", s"$home/run.err"))
          if (runExit != 0) throw new RuntimeException(s"Experiment run command exited with non-zero code")

          // copy logs
          shell ! s"rm -Rf $home/stratosphere-logs/*"

          // copy new lines in the log and out files
          shell ! s"rm -Rf $home/stratosphere-logs/*"
          for (e <- logFiles)
            shell ! s"tail -n +${e._2} ${e._1} > $home/stratosphere-logs/${Paths.get(e._1).getFileName}"

          // update run state
          state.time = t
          state.exit = Some(runExit)

          logger.info(s"Experiment run executed in $t milliseconds")
        } catch {
          case e: Exception => logger.error("Exception in experiment run %s: %s".format(name, e.getMessage))
        } finally {
          writeState()
        }
      }
    }

    private def ensureFolderIsWritable(folder: Path) = {
      if (Files.exists(folder)) {
        if (!(Files.isDirectory(folder) && Files.isWritable(folder))) throw new RuntimeException(s"Experiment home '$home' is not a writable directory")
      } else {
        Files.createDirectories(folder)
      }
    }

    private def loadState(): State = {
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

    private def writeState() = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }
  }

  def time[R](block: => R): (R, Long) = {
    val t0 = System.currentTimeMillis
    val result = block
    val t1 = System.currentTimeMillis
    (result, t1 - t0)
  }
}
