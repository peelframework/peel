package eu.stratosphere.peel.core.beans.data

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}

import eu.stratosphere.peel.core.beans.system.{FileSystem, System}

/** Dataset that is copied from a local filesystem to a specified target location.
  *
  * If the data already exists at the specified location, it is '''not''' copied again!
  *
  * @param src Local path where the data is stored.
  * @param dst Path in the distributed filesystem where the data is stored.
  * @param fs The filesystem that is used.
  */
class CopiedDataSet(val src: String, val dst: String, val fs: System with FileSystem) extends DataSet(dst, Set[System](fs)) {

  import scala.language.implicitConversions

  override def materialize() = {
    // resolve parameters from the current config in src and dst
    val dst = resolve(this.dst)
    val src = resolve(this.src)

    if (!Files.isRegularFile(Paths.get(src))) {
      throw new FileNotFoundException(s"Local static file at location '$src' does not exist!")
    }

    logger.info(s"Copying data set '$src' to '$dst'")
    if (fs.copyFromLocal(src, dst) != 0) throw new RuntimeException(s"Could not copy '$src' to '$dst'")
  }
}
