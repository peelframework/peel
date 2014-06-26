package eu.stratosphere.peel.extensions.beans.system.data.hadoop

import java.io.FileNotFoundException
import java.nio.file.{Paths, Files}

import eu.stratosphere.peel.core.beans.data.DataSet
import eu.stratosphere.peel.core.beans.system.System
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.beans.system.hadoop.HDFS

class StaticHDFSDataSet(val src: String, val dst: String, hdfs: HDFS) extends DataSet(dst, Set[System](hdfs)) {

  import scala.language.implicitConversions

  override def materialize() = {
    // resolve parameters from the current config in src and dst
    val dst = resolve(this.dst)
    val src = resolve(this.src)

    // fetch relevant config entries
    val hadoopHome = config.getString("system.hadoop.path.home")

    if (!exists(dst)) {
      if (!Files.isRegularFile(Paths.get(src))) {
        throw new FileNotFoundException(s"Local static file at location '$src' does not exist!")
      }

      logger.info(s"Copying data set '$src' to '$dst'")

      if (src.endsWith(".gz")) {
        shell ! s"""gunzip -c \"$src\" | $hadoopHome/bin/hadoop fs -put - \"$dst\" """
      } else {
        shell ! s"""$hadoopHome/bin/hadoop fs -copyFromLocal "$src" "$dst" """
      }
    }
  }

  private def exists(path: String) = {
    val hadoopHome = config.getString("system.hadoop.path.home")
    shell !! s"""if $hadoopHome/bin/hadoop fs -test -e "$path" ; then echo "YES" ; else echo "NO"; fi""" == "YES"
  }
}
