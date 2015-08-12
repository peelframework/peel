/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
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
package org.peelframework.core.cli.command.hosts

import java.nio.charset.StandardCharsets
import java.nio.file.{Paths, Files}

import org.peelframework.core.cli.command.Command
import org.peelframework.core.cli.command.hosts.Generate.Interval
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Generate a hosts.conf file. */
@Service("hosts:generate")
class Generate extends Command {

  override val name = "hosts:generate"

  override val help = "generate a hosts.conf file"

  var ns: Namespace = null

  override def register(parser: Subparser) = {
    // master specification
    {
      val group = parser.addArgumentGroup("masters specification")
      group.addArgument("--masters")
        .`type`(classOf[String])
        .dest("app.hosts.generate.masters")
        .metavar("URL")
        .nargs("+")
        .help("master URLs")
    }

    // index-based slaves specification
    {
      val group = parser.addArgumentGroup("slaves specification (index-based)")
      group.addArgument("--slaves-pattern")
        .`type`(classOf[String])
        .dest("app.hosts.generate.slaves.pattern")
        .metavar("PATTERN")
        .help("slaves URL pattern")
      group.addArgument("--slaves-include")
        .`type`(classOf[Interval])
        .dest("app.hosts.generate.slaves.include")
        .metavar("[MIN, MAX]")
        .nargs("*")
        .help("include slaves range")
      group.addArgument("--slaves-exclude")
        .`type`(classOf[Interval])
        .dest("app.hosts.generate.slaves.exclude")
        .metavar("[MIN, MAX]")
        .nargs("*")
        .help("exclude slaves range")
    }

    // file-based slaves specification
    {
      val group = parser.addArgumentGroup("slaves specification (file-based)")
      group.addArgument("--slaves-list")
        .`type`(Arguments.fileType().acceptSystemIn().verifyCanRead())
        .dest("app.hosts.generate.slaves.file")
        .metavar("FILE")
        .help("slaves list file")
    }

    // master specification
    {
      val group = parser.addArgumentGroup("resource specification")
      // master
      group.addArgument("--parallelism")
        .`type`(classOf[Int])
        .dest("app.hosts.generate.parallelism")
        .metavar("INT")
        .help("parallelism per node")

      // master
      group.addArgument("--memory")
        .`type`(classOf[Long])
        .dest("app.hosts.generate.memory")
        .metavar("LONG")
        .help("memory per node (in kB)")
    }

    // master specification
    {
      val group = parser.addArgumentGroup("slave groups")
      // master
      group.addArgument("--unit")
        .`type`(classOf[Int])
        .dest("app.hosts.generate.unit")
        .metavar("INT")
        .help("unit size 'a' for 'an^i' progression")
    }

    // master specification
    {
      val group = parser.addArgumentGroup("output")
      // master
      group.addArgument("output")
        .`type`(Arguments.fileType().verifyCanCreate())
        .dest("app.hosts.generate.output")
        .metavar("FILE")
        .help("output file")
        .required(true)
    }

    // option defaults
    parser.setDefault("app.hosts.generate.slaves.include", Seq("[0, 10]").toList.asJava)
    parser.setDefault("app.hosts.generate.slaves.exclude", Seq.empty[String].toList.asJava)
    parser.setDefault("app.hosts.generate.masters", Seq("localhost").toList.asJava)
    parser.setDefault("app.hosts.generate.unit", 2)
  }

  override def configure(ns: Namespace) = {
    this.ns = ns
    // set ns options and arguments to system properties
    // Sys.setProperty("app.rsync.remote.name", ns.getString("app.rsync.remote.name"))
  }

  override def run(context: ApplicationContext) = {

    val masters     = ns.getList[String]("app.hosts.generate.masters").asScala
    val slaves      = getSlaves(ns)
    val parallelism = ns.getInt("app.hosts.generate.parallelism").toInt
    val memory      = ns.getLong("app.hosts.generate.memory").toLong
    val unit        = ns.getInt("app.hosts.generate.unit").toInt
    val output      = Paths.get(ns.getString("app.hosts.generate.output")).normalize.toAbsolutePath

    logger.info(s"Generating hosts file '$output'")

    val sb          = StringBuilder.newBuilder
    sb.append(
      s"""
        |###############################################################################
        |# Auto-generated hosts.conf
        |# Input command was:
        |#
        |# ${getCommand(ns).mkString("# ").trim}
        |# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        |#
        |
        |env {
        |    masters = [${masters.map(x => s""""$x"""").mkString(", ")}]
        |    per-node= {
        |        parallelism = $parallelism
        |        memory = $memory
        |    }
        |    slaves {
        |        # all slaves
        |        all = {
        |            total = {
        |                hosts = ${slaves.size}
        |                parallelism = ${slaves.size * parallelism}
        |                memory = ${slaves.size * memory}
        |            }
        |            hosts = [
        |                ${(for (s <- slaves) yield s""""$s"""").mkString(",\n|" + " " * 16)}
        |            ]
        |        }
      """.trim.stripMargin + "\n")

    var k = unit
    while (k <= slaves.size) {
      val n = f"top$k%03d"

      sb.append(
        s"""
          |        # top $k slaves
          |        $n = {
          |            total = {
          |                hosts = $k
          |                parallelism = ${k * parallelism}
          |                memory = ${k * memory}
          |            }
          |            hosts = [
          |                ${(for (s <- slaves.slice(0, k)) yield s""""$s"""").mkString(",\n|" + " " * 16)}
          |            ]
          |        }
        """.trim.stripMargin + "\n")

      k = k * 2 // advance k
    }


    sb.append(
      """
        |    }
        |}
      """.trim.stripMargin + "\n")


    Files.createDirectories(output.getParent)
    Files.write(output, sb.result().getBytes(StandardCharsets.UTF_8))
  }

  def getSlaves(ns: Namespace): List[String] = {
    if (ns.getString("app.hosts.generate.slaves.file") != null) {
      scala.io.Source.fromFile(ns.getString("app.hosts.generate.slaves.file")).getLines().toList
    }
    else if (ns.getString("app.hosts.generate.slaves.pattern") != null) {
      // get pattern
      val pattern = ns.getString("app.hosts.generate.slaves.pattern")
      // get included range
      val include = {
        val xs = ns.getList[Interval]("app.hosts.generate.slaves.include").asScala
        xs.foldLeft(IndexedSeq.empty[Int])((r, x) => r union x.asRange)
      }
      // get excluded range
      val exclude = {
        val xs = ns.getList[Interval]("app.hosts.generate.slaves.exclude").asScala
        xs.foldLeft(IndexedSeq.empty[Int])((r, x) => r union x.asRange)
      }
      // compute slaves
      (for (i <- include diff exclude) yield String.format(pattern, new java.lang.Integer(i))).toList
    } else {
      throw new IllegalStateException("One of 'slaves-pattern' or 'slaves-file' must be provided")
    }
  }

  def getCommand(ns: Namespace) = {
    val masters       = ns.getList[String]("app.hosts.generate.masters").asScala
    val slavesFile    = ns.getString("app.hosts.generate.slaves.file")
    val slavesPattern = ns.getString("app.hosts.generate.slaves.pattern")
    val slavesInclude = ns.getList[Interval]("app.hosts.generate.slaves.include").asScala
    val slavesExclude = ns.getList[Interval]("app.hosts.generate.slaves.exclude").asScala
    val parallelism   = ns.getInt("app.hosts.generate.parallelism").toInt
    val memory        = ns.getLong("app.hosts.generate.memory").toLong
    val unit          = ns.getInt("app.hosts.generate.unit").toInt
    val output        = ns.getString("app.hosts.generate.output")

    val sb            = ListBuffer.newBuilder[String]

    sb += s"""hosts:generate $output""".padTo(76, ' ') + "\\\n"
    if (masters != null) {
      sb += s"""  --masters ${masters.map(x => s""""$x"""").mkString(" ")}""".padTo(76, ' ') + "\\\n"
    }
    if (slavesFile != null) {
      sb += s"""  --slaves-file "$slavesFile"""".padTo(76, ' ') + "\\\n"
    }
    if (slavesPattern != null) {
      sb += s"""  --slaves-pattern "$slavesPattern"""".padTo(76, ' ') + "\\\n"
    }
    if (slavesInclude != null && slavesInclude.nonEmpty) {
      sb += s"""  --slaves-include ${slavesInclude.mkString(" ")}""".padTo(76, ' ') + "\\\n"
    }
    if (slavesExclude != null && slavesExclude.nonEmpty) {
      sb += s"""  --slaves-exclude ${slavesExclude.mkString(" ")}""".padTo(76, ' ') + "\\\n"
    }
    sb += s"""  --parallelism $parallelism""".padTo(76, ' ') + "\\\n"
    sb += s"""  --memory $memory""".padTo(76, ' ') + "\\\n"
    sb += s"""  --unit $unit""" + "\n"

    sb.result()
  }
}

object Generate {

  val range = """\[(\d+)\W*,\W*(\d+)\]""".r

  case class Interval(v: String) {

    def asRange = min to max

    private def min: Int = v match {
      case range(min, max) => min.toInt
      case _ => throw new IllegalArgumentException(s"Illegal range string '$v', expected [MIN, MAX]")
    }

    private def max: Int = v match {
      case range(min, max) => max.toInt
      case _ => throw new IllegalArgumentException(s"Illegal range string '$v', expected [MIN, MAX]")
    }

    override def toString = v
  }

}
