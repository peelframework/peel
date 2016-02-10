/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    val HDFS = """^hdfs://.*""".r
    src match {
      case HDFS(_*) =>
        shell !(s"""$hadoopHome/bin/hadoop distcp -update -skipcrccheck $src $dst """, "Unable to copy file between HDFS instances.")

      case _ =>
        if (src.endsWith(".gz"))
          shell !( s"""gunzip -c \"$src\" | $hadoopHome/bin/hadoop fs -put - \"$dst\" """, "Unable to copy file to HDFS.")
        else
          shell !( s"""$hadoopHome/bin/hadoop fs -copyFromLocal "$src" "$dst" """, "Unable to copy file to HDFS.")
    }
  }

  def mkdir(dir: String) = {
    val hadoopHome = config.getString(s"system.$configKey.path.home")
    shell !( s"""$hadoopHome/bin/hadoop fs -mkdir -p "$dir" """, "Unable to create directory in HDFS.")
  }
}
