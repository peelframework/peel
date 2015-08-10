package org.peelframework.hadoop.beans.system

import org.peelframework.core.beans.system.{FileSystem, System}
import org.peelframework.core.util.shell
import org.peelframework.core.util.Version
import org.peelframework.core.beans.system.FileSystem

private[system] trait HDFSFileSystem extends FileSystem {
  self: System =>

  override def exists(path: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    (shell !! s"""if $hadoopHome/bin/hadoop fs -test -e "$path" ; then echo "YES" ; else echo "NO"; fi""").trim() == "YES"
  }

  override def rmr(path: String, skipTrash: Boolean = true) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    // assemble options
    val opts = Seq(
      if (Version(version) < Version("2")) Some("-rmr") else Some("-rm -r"),
      if (skipTrash) Some("-skipTrash") else Option.empty[String]
    )

    // assemble command
    val cmd =
      s"""
       |if $hadoopHome/bin/hadoop fs -test -e "$path"; then
       |  $hadoopHome/bin/hadoop fs ${opts.flatten.mkString(" ")} "$path";
       |fi
       """.stripMargin.trim

    // execute command
    shell !(cmd, "Unable to remove path in HDFS.", fatal = false)
  }

  override def copyFromLocal(src: String, dst: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    if (src.endsWith(".gz"))
      shell !( s"""gunzip -c \"$src\" | $hadoopHome/bin/hadoop fs -put - \"$dst\" """, "Unable to copy file to HDFS.")
    else
      shell !( s"""$hadoopHome/bin/hadoop fs -copyFromLocal "$src" "$dst" """, "Unable to copy file to HDFS.")
  }

  def mkdir(dir: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    shell !( s"""$hadoopHome/bin/hadoop fs -mkdir -p "$dir" """, "Unable to create directory in HDFS.")
  }
}
