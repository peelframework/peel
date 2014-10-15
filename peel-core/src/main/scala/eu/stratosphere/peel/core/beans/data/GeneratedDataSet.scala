package eu.stratosphere.peel.core.beans.data



import eu.stratosphere.peel.core.beans.system.{Job, FileSystem, System}

class GeneratedDataSet(val src: Job, val dst: String, fs: System with FileSystem) extends DataSet(dst, Set[System](fs)) {
  import scala.language.implicitConversions

  override def materialize() = {
    // resolve parameters from the current config in dst
    val dst = resolve(this.dst)

    if (!fs.exists(dst)) {
      logger.info(s"Generating data set '${src.command}' to '$dst'")
      try {
        src.execute()
      } catch {
        case e: Throwable => throw new RuntimeException(s"Could not generate the dataset with '${src.command}' to '$dst'")
      }
    }
  }
}
