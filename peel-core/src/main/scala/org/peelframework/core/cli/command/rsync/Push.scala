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
package org.peelframework.core.cli.command.rsync

import java.lang.{System => Sys}
import java.nio.file.Paths

import org.peelframework.core.cli.command.Command
import org.peelframework.core.config._
import org.peelframework.core.util.shell
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

/** Push bundle to a remote location. */
@Service("rsync:push")
class Push extends Command {

  override val name = "rsync:push"

  override val help = "push bundle to a remote location"

  override def register(parser: Subparser) = {
    // arguments
    parser.addArgument("remote")
      .`type`(classOf[String])
      .dest("app.rsync.remote.name")
      .metavar("ID")
      .help("remote config name")
  }

  override def configure(ns: Namespace) = {
    // set ns options and arguments to system properties
    Sys.setProperty("app.rsync.remote.name", ns.getString("app.rsync.remote.name"))
  }

  override def run(context: ApplicationContext) = {
    val remote = Sys.getProperty("app.rsync.remote.name") // get suite from command parameter

    val config = loadConfig() // load application configuration at source

    val bun = Paths.get(config.getString("app.path.home")).getFileName.toString // bundle name
    val url = config.getString(s"app.rsync.$remote.url") // remote host url
    val rsh = config.getString(s"app.rsync.$remote.rsh") // remote shell
    val dst = config.getString(s"app.rsync.$remote.dst") // remote destination base folder

    val dirs = Seq(
      FolderEntry(
        src = Paths.get(config.getString("app.path.home")),
        dst = Paths.get(dst, bun),
        inc = Seq("peel.sh", "VERSION", "NOTICE*", "LICENSE*", "README*"),
        exc = Seq("*")),
      FolderEntry(
        src = Paths.get(config.getString("app.path.apps")),
        dst = Paths.get(dst, bun, "apps"),
        inc = Seq("*"),
        exc = Seq.empty[String]),
      FolderEntry(
        src = Paths.get(config.getString("app.path.config")),
        dst = Paths.get(dst, bun, "config"),
        inc = Seq("*"),
        exc = Seq.empty[String]),
      FolderEntry(
        src = Paths.get(config.getString("app.path.datagens")),
        dst = Paths.get(dst, bun, "datagens"),
        inc = Seq("*"),
        exc = Seq.empty[String]),
      FolderEntry(
        src = Paths.get(config.getString("app.path.datasets")),
        dst = Paths.get(dst, bun, "datasets"),
        inc = Seq("*"),
        exc = Seq.empty[String]),
      FolderEntry(
        src = Paths.get(config.getString("app.path.downloads")),
        dst = Paths.get(dst, bun, "downloads"),
        inc = Seq.empty[String],
        exc = Seq("*")),
      FolderEntry(
        src = Paths.get(config.getString("app.path.home"), "log"),
        dst = Paths.get(dst, bun, "log"),
        inc = Seq.empty[String],
        exc = Seq("*")),
      FolderEntry(
        src = Paths.get(config.getString("app.path.home"), "lib"),
        dst = Paths.get(dst, bun, "lib"),
        inc = Seq("*"),
        exc = Seq.empty[String]),
      FolderEntry(
        src = Paths.get(config.getString("app.path.results")),
        dst = Paths.get(dst, bun, "results"),
        inc = Seq.empty[String],
        exc = Seq("*")),
      FolderEntry(
        src = Paths.get(config.getString("app.path.systems")),
        dst = Paths.get(dst, bun, "systems"),
        inc = Seq.empty[String],
        exc = Seq("*")),
      FolderEntry(
        src = Paths.get(config.getString("app.path.utils")),
        dst = Paths.get(dst, bun, "utils"),
        inc = Seq.empty[String],
        exc = Seq("*"))
    )

    for (entry <- dirs) {
      logger.info(s"Pushing '${entry.src}' to '${entry.dst}' (include: ${entry.inc.mkString(" ")}, exclude: ${entry.exc.mkString(" ")})")
      val includes = entry.inc.flatMap(x => Seq("--include", s"'$x'")).mkString(" ")
      val excludes = entry.exc.flatMap(x => Seq("--exclude", s"'$x'")).mkString(" ")
      shell !(
        s"""rsync -L -a -v -r -e "$rsh" $includes $excludes ${entry.src}/. $url:${entry.dst}""",
        s"""Error while syncing '${entry.src}' with '${entry.dst}""",
        fatal = false)
    }

    for {
      hom <- Some(Paths.get(dst, bun)) // destination home
      own <- config.getOptionalString(s"app.rsync.$remote.own") // owner
    } {
      logger.info(s"Changing owner of '${Paths.get(dst, bun)}' to '$own'")
      shell !(
        s"""$rsh $url chown -R $own "${Paths.get(dst, bun)}" """,
        s"""Error while changing owner of '${Paths.get(dst, bun)}'""",
        fatal = false)
    }
  }
}
