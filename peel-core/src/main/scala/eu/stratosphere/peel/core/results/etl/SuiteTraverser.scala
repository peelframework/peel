package eu.stratosphere.peel.core.results.etl

import java.io.File
import java.nio.file.{Files, Paths}

import com.typesafe.config.Config
import spray.json._

/** Traverse the state.json entries of all runs within a given suite.
  *
  * @param suite The suite to be traversed.
  * @param config The application configuration.
  */
class SuiteTraverser(suite: Symbol)(implicit config: Config) extends Traversable[RunState] {

  import SuiteTraverser.loadState

  val resultsPath = Paths.get(config.getString("app.path.results")).normalize.toAbsolutePath
  val suitePath = Paths.get(resultsPath.toString, suite.name)

  override def foreach[U](f: RunState => U): Unit = for {
    runPath <- suitePath.toFile.listFiles.sortBy(_.getAbsolutePath).map(_.getAbsoluteFile) if runPath.isDirectory
    runState <- loadState(runPath)
    runExitCode <- runState.runExitCode
  } f(runState)
}

object SuiteTraverser {

  def apply(suite: Symbol)(implicit config: Config) = new SuiteTraverser(suite)

  private implicit val RunStateFormat = RunStateProtocol.stateFormat

  private def loadState(runPath: File): Option[RunState] = {
    if (Files.isRegularFile(Paths.get(s"${runPath.getAbsolutePath}/state.json"))) {
      try {
        Some(io.Source.fromFile(s"${runPath.getAbsolutePath}/state.json").mkString.parseJson.convertTo[RunState])
      } catch {
        case e: Throwable => Option.empty[RunState]
      }
    } else {
      Option.empty[RunState]
    }
  }
}