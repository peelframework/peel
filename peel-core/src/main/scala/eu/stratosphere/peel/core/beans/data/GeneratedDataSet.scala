package eu.stratosphere.peel.core.beans.data

import eu.stratosphere.peel.core.beans.system.{FileSystem, Job, System}

class GeneratedDataSet(val src: Job[System], val dst: String, fs: System with FileSystem) extends DataSet(dst, Set[System](fs)) {

  import scala.language.implicitConversions

  override def materialize() = {
    // resolve parameters from the current config in dst
    val dst = resolve(this.dst)

    if (!fs.exists(dst)) {
      logger.info(s"Generating data for target '$dst'")
      try {
        src.execute()
      } catch {
        case e: Throwable => throw new RuntimeException(s"Could not generate data for target '$dst'")
      }
    }
  }
}