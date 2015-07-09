package eu.stratosphere.peel.extensions.hadoop.beans.system

import eu.stratosphere.peel.core.beans.system.{System, FileSystem}
import eu.stratosphere.peel.core.util.shell

private[system] trait HDFSFileSystem extends FileSystem {
  self: System =>

  override def exists(path: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    (shell !! s"""if $hadoopHome/bin/hadoop fs -test -e "$path" ; then echo "YES" ; else echo "NO"; fi""").trim() == "YES"
  }

  override def rmr(path: String, skipTrash: Boolean = true) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    if (skipTrash)
      shell ! (s"""$hadoopHome/bin/hadoop fs -rmr -skipTrash "$path" """, "Unable to remove path in HDFS.")
    else
      shell ! (s"""$hadoopHome/bin/hadoop fs -rmr "$path" """, "Unable to remove path in HDFS.")
  }

  override def copyFromLocal(src: String, dst: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    if (src.endsWith(".gz"))
      shell ! (s"""gunzip -c \"$src\" | $hadoopHome/bin/hadoop fs -put - \"$dst\" """, "Unable to copy file to HDFS.")
    else
      shell ! (s"""$hadoopHome/bin/hadoop fs -copyFromLocal "$src" "$dst" """, "Unable to copy file to HDFS.")
  }

  def mkdir(dir: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    shell ! (s"""$hadoopHome/bin/hadoop fs -mkdir -p "$dir" """, "Unable to create directory in HDFS.")
  }
}
