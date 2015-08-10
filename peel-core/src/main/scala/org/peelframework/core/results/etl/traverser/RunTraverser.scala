package org.peelframework.core.results.etl.traverser

import java.io.File
import java.nio.file.Path

import com.typesafe.config.Config
import org.peelframework.core.util.shell.fileTree

/** Traverse the file entries for a state run.
  *
  * @param runPath The path to be traversed.
  * @param config The application configuration.
  */
class RunTraverser(runPath: Path)(implicit config: Config) extends Traversable[File] {

  override def foreach[U](f: File => U): Unit = for {
    file <- fileTree(runPath.toFile) if file.isFile && file.canRead
  } f(file)
}

/** Companion object. */
object RunTraverser {

  def apply(runPath: Path)(implicit config: Config) = new RunTraverser(runPath)
}
