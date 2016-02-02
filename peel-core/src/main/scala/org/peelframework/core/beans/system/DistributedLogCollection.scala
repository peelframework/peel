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
package org.peelframework.core.beans.system

import java.io.File
import java.nio.file.{Files, Paths}

import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.config.RichConfig
import org.peelframework.core.util.shell

import scala.collection.Seq
import scala.util.matching.Regex

/** A trait that implements log collection behaviour for systems.
  *
  * In difference to [[LogCollection]] the logs are copied via ssh, thus it does not require a
  * shared folder.
  *
  * Subclasses of this trait must provide a list of `slaves` which are used to gather the logs via ssh.
  */
trait DistributedLogCollection {
  self: System =>

  def slaves: Seq[String]

  case class FileEntry(slave: String, file: String)

  /** Log file line counts at the beginning of the last run. */
  private var logFileCounts: Map[FileEntry, Long] = null

  /** Executed before each experiment run that depends on this system. */
  override def beforeRun(run: Run[System]): Unit = {
    // handle dependencies
    for (s <- dependencies) {
      s.beforeRun(run)
    }
    // get the log path of this system
    implicit val patterns = logFilePatterns()
    // collect relevant log files
    val logFiles = for {
      slave <- slaves
      logs <- config.getOptionalString(s"system.$configKey.path.log").toList
      file <- (shell !! s"""ssh $slave "find $logs -type f -exec ls {} \\; 2> /dev/null"""".trim).split('\n')
      if canProcess(new File(file))
    } yield FileEntry(slave, file)
    // collect current log file counts
    logFileCounts = (for (fileEntry <- logFiles) yield {
        fileEntry -> (shell !! s"""ssh ${fileEntry.slave} "wc -l ${fileEntry.file} | xargs | cut -d' ' -f1"""").trim.toLong
    }).toMap
  }



  /** Executed after each experiment run that depends on this system. */
  override def afterRun(run: Run[System]): Unit = {
    // ensure target folder where log should be stored exists, is empty and is writable
    for (folder <- Some(Paths.get(run.home, "logs", name, beanName))) {
      shell.ensureFolderIsWritable(folder)
      shell.rm(folder.toString)
      shell.ensureFolderIsWritable(folder)
    }
    // dump the new files in the log folder
    for ((entry, count) <- logFileCounts) {
      shell ! s"""ssh ${entry.slave} "tail -n +${count + 1} ${entry.file}" > ${run.home}/logs/$name/$beanName/${Paths.get(entry.file).getFileName}"""
    }
    // handle dependencies
    for (s <- dependencies) {
      s.afterRun(run)
    }
  }

  /** The patterns of the log files to watch. */
  protected def logFilePatterns(): Seq[Regex]

  /** A file can be processed if and only if at least one of the file patterns matches. */
  private def canProcess(file: File)(implicit patterns: Seq[Regex]): Boolean = {
    patterns.exists(_.pattern.matcher(file.getName).matches())
  }
}
