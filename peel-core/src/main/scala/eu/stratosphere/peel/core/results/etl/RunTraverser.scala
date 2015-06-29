package eu.stratosphere.peel.core.results.etl

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.Config

/** Traverse the state.json entries of all runs within a given suite.
  *
  * @param state The suite to be traversed.
  * @param config The application configuration.
  */
class RunTraverser(state: RunState)(implicit config: Config) extends Traversable[File] {

  val resultsPath = Paths.get(config.getString("app.path.results")).normalize.toAbsolutePath

  override def foreach[U](f: File => U): Unit = for {
    runPath <- Some(Paths.get(resultsPath.toString, state.suiteName, state.name).toFile)
    runFile <- runPath.listFiles.sortBy(_.getAbsolutePath).map(_.getAbsoluteFile) if runPath.isFile
  } f(runFile)
}

object RunTraverser {

  def apply(state: RunState)(implicit config: Config) = new RunTraverser(state)
}