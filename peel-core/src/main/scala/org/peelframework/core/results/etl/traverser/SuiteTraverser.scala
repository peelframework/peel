package org.peelframework.core.results.etl.traverser

import java.io.File
import java.nio.file.Path

import com.typesafe.config.Config
import org.peelframework.core.results.etl.{RunState, RunStateProtocol}
import org.peelframework.core.results.etl.RunState
import resource._
import spray.json._

/** Traverse the state.json entries of all runs within a given suite path.
  *
  * @param suitePath The suite path to be traversed.
  * @param config The application configuration.
  */
class SuiteTraverser(suitePath: Path)(implicit config: Config) extends Traversable[RunState] {

  import SuiteTraverser.loadState

  override def foreach[U](f: RunState => U): Unit = for {
    dir <- suitePath.toFile.listFiles.sortBy(_.getAbsolutePath) if dir.isDirectory
    fil <- Option(new File(dir, "state.json")) if fil.isFile
    run <- loadState(fil)
  } f(run)
}

/** Companion object. */
object SuiteTraverser {

  private implicit val RunStateFormat = RunStateProtocol.stateFormat

  def apply(suitePath: Path)(implicit config: Config) = new SuiteTraverser(suitePath)

  private def loadState(runFile: File): Option[RunState] = {
    (for {
      src <- managed(scala.io.Source.fromFile(runFile))
    } yield src.mkString.parseJson.convertTo[RunState]).opt
  }
}
