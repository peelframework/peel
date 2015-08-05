package eu.stratosphere.peel.extensions.cassandra.beans.experiment

import java.lang.{System => Sys}
import java.io.FileWriter
import java.nio.file.{Paths, Files}

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{ExperimentOutput, DataSet}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.cassandra.beans.system.Cassandra
import spray.json._

/** An [[eu.stratosphere.peel.core.beans.experiment.Experiment Experiment]] implementation which handles the execution
  * of a single Cassandra job.
  *
  */
class CassandraExperiment(
                     command: String,
                     runner : Cassandra,
                     runs   : Int,
                     inputs : Set[DataSet],
                     outputs: Set[ExperimentOutput],
                     name   : String,
                     config : Config) extends Experiment(command, runner, runs, inputs, outputs, name, config) {

  def this(
            runs   : Int,
            runner : Cassandra,
            input  : DataSet,
            output : ExperimentOutput,
            command: String,
            name   : String,
            config : Config) = this(command, runner, runs, Set(input), Set(output), name, config)

  def this(
            runs   : Int,
            runner : Cassandra,
            inputs : Set[DataSet],
            output : ExperimentOutput,
            command: String,
            name   : String,
            config : Config) = this(command, runner, runs, inputs, Set(output), name, config)

  override def run(id: Int, force: Boolean): Experiment.Run[Cassandra] = new CassandraExperiment.SingleJobRun(id, this, force)

  def copy(name: String = name, config: Config = config) = new CassandraExperiment(command, runner, runs, inputs, outputs, name, config)

}

object CassandraExperiment {

  case class State(
                    name: String,
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

  /** A private inner class encapsulating the logic of single run. */
  class SingleJobRun(val id: Int, val exp: CassandraExperiment, val force: Boolean) extends Experiment.SingleJobRun[Cassandra, State] {

    import eu.stratosphere.peel.extensions.cassandra.beans.experiment.CassandraExperiment.StateProtocol._

    val runnerLogPath = s"${exp.config.getString("system.cassandra.path.log")}"

    override def isSuccessful = state.runExitCode.getOrElse(-1) == 0

    override protected def logFilePatterns = List(s"$runnerLogPath/system.log")

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

    override protected def writeState() = {
      val fw = new FileWriter(s"$home/state.json")
      fw.write(state.toJson.prettyPrint)
      fw.close()
    }

    override protected def runJob() = {
      // try to execute the experiment
      val (runExit, t) = Experiment.time(this ! (command, s"$home/run.out", s"$home/run.err"))
      state.runTime = t
      state.runExitCode = Some(runExit)
    }

    /** After the run, copy logs, clear the cassandra database */
    override protected def afterRun(): Unit = {
      super.afterRun()
      val pollingNode = exp.config.getStringList("system.cassandra.config.slaves").get(0)
      val port = exp.config.getString("system.cassandra.config.env.JMX_PORT")
      val keyspaces = shell !! s"echo desc keyspaces | ${exp.config.getString("system.cassandra.path.home")}/bin/cqlsh | xargs -n1 echo | grep -v ^system"
      for (keyspace <- keyspaces.split("\n")) {
        shell ! s"echo 'drop keyspace $keyspace;' | ${exp.config.getString("system.cassandra.path.home")}/bin/cqlsh"
      }
    }

    override def cancelJob() = {
     //TODO: fix me
    }

    private def !(command: String, outFile: String, errFile: String) = {
      shell ! s"$command > $outFile 2> $errFile"
    }
  }
}