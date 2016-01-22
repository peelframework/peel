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
package org.peelframework.dstat.beans.system

import java.nio.file.{StandardOpenOption, Files, Paths}

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.experiment.Experiment.Run
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.{LogCollection, System}
import org.peelframework.core.config.SystemConfig
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.matching.Regex

/**
 * Implements Dstat as a Peel system.
 *
 * The options for the `dstat` command can be configured using `system.${configKey}.cli.options`.
 * Per default, if `system.\${configKey}.cli.options` is not set, the options are generated as follows:
 * <p/>
 * The options equal to `--epoch --cpu -C $cpuList --mem --net -N $netList --disk -D $diskList --noheaders --nocolor`
 * where `$cpuList` (`$netList`, `$diskList`) are either substituted by
 * <ul>
 *   <li>`system.${configKey}.cli.cpu.list` (`system.${configKey}.cli.net.list`, `system.${configKey}.cli.disk.list`) if set</li>
 *   <li>or generated otherwise including the `total` statistics as well as all cores (interfaces, devices)</li>
 * </ul>
 *
 * @param version Version of the system (e.g. "7.1")
 * @param configKey The system configuration resides under `system.${configKey}`
 * @param lifespan `Lifespan` of the system
 * @param dependencies Set of dependencies that this system needs
 * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
 */
class Dstat(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("dstat", version, configKey, lifespan, dependencies, mc)
                                       with LogCollection {

//  var pids: Map[String, String] = Map.empty

  override protected def logFilePatterns(): Seq[Regex] = {
    List("dstat-.+\\.csv".r)
  }


  override protected def configuration(): SystemConfig = {
    SystemConfig(config, List.empty)
  }

  override protected def stop(): Unit = {
    val pidDir = Paths.get(s"${config.getString("system.flink.config.yaml.env.pid.dir")}/dstat.pid")
    if (Files.exists(pidDir)) {
      Closeable.guard(Files.newBufferedReader(pidDir)) on { reader =>
        while(reader.ready()) {
          val v = reader.readLine().split(',')
          shell ! s""" ssh ${v(0)} "kill ${v(1)} 2>/dev/null >/dev/null" """
        }
      }
    }
  }

  // TODO how to handle partial state?
  // currently this works because `isRunning` is used when shutting down systems
  override def isRunning: Boolean = {
    val pidDir = Paths.get(s"${config.getString("system.flink.config.yaml.env.pid.dir")}/dstat.pid")
    Files.exists(pidDir) && (
      Closeable.guard(Files.newBufferedReader(pidDir)) on { reader =>
        var ret = true
        while(reader.ready()) {
          val v = reader.readLine().split(',')
          ret = ret & (shell ! s""" ssh ${v(1)} "ps -p ${v(2)}" """) == 0
        }
        ret
      }
    )
  }

  override protected def start(): Unit = {
  }

  def savePids(pids: Map[String, String]) = {
    val pidDir = Paths.get(s"${config.getString("system.flink.config.yaml.env.pid.dir")}/dstat.pid")
    require(!Files.exists(pidDir))
    Closeable.guard { Files.newBufferedWriter(pidDir) } on { writer =>
      for ((slave, pid) <- pids) {
        writer.write(s"$slave,$pid")
        writer.newLine()
      }
    }
  }

  private def _start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")
    val dstat = config.getString(s"system.$configKey.path.home") + "/dstat"

    logger.info(s"user: $user, logDir: $logDir, dstat: $dstat")

    shell.ensureFolderIsWritable(Paths.get(logDir))
    val pids = (for (slave <- config.getStringList(s"system.$configKey.config.slaves").asScala) yield {
      logger.info(s"reached $slave")
      val options = buildOptions(slave)
      val cmd = s"$dstat $options --output $logDir/dstat-$user-$slave.csv 1"

      logger.info(s"""Executing "$cmd" on host "$slave"""")
      val ssh = s""" ssh $slave "nohup $cmd >/dev/null 2>/dev/null & echo \\$$!" """
      val dstatPid = shell !! ssh //shell !! s""" ssh $slave "nohup $cmd >/dev/null 2>/dev/null & echo \\$$!" """
      logger.info(ssh)
      logger.info(s"Dstat started on $slave with PID $dstatPid")

      (slave, dstatPid)
    }).toMap

    savePids(pids)
  }

  private def touch(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")
    for (slave <- config.getStringList(s"system.$configKey.config.slaves").asScala) yield {
      val cmd = s"touch $logDir/dstat-$user-$slave.csv"
      shell ! s""" ssh $slave "$cmd" """
    }
  }

  override def beforeRun(run: Run[System]): Unit = {
    // delegate to parent
    touch()
    super[LogCollection].beforeRun(run)
    _start()
  }

  /**
   * Generates the options for dstat if not set.
   * Heavily inspired by <a href="https://github.com/mvneves/dstat-monitor">https://github.com/mvneves/dstat-monitor</a>.
   *
   * @param slave the machine for which the options should be generated.
   * @return the options string.
   */
  def buildOptions(slave: String): String = {
    val dstat = config.getString(s"system.$configKey.path.home") + "/dstat"

    // if options is set, then use it
    val options = config.getString(s"system.$configKey.cli.options")
    if (options != "") {
      return options
    }

    // if options is empty, then build the options
    var cpuList = config.getString(s"system.$configKey.cli.cpu.list")
    if (cpuList == "") {
      val cpuListCmd = s"$dstat --cpu --full --nocolor 1 0 | head -n 1 | tr -d '-' | tr ' ' '\\n' | wc -l"
      val nCpu = Integer.parseInt((shell !! s""" ssh $slave "$cpuListCmd" """).trim)
      cpuList = "total," + (1 to nCpu).map(x => x.toString).reduce((s1, s2) => s"$s1,$s2")
    }

//    ifaces=`$dstat --net --full --nocolor 1 0 | head -n 1 | tr -d '-' | sed 's/net\///g'`
//    iface_list="total"
//    for i in $ifaces
//    do
//      eth=`echo $i | sed -r 's/.*eth([0-9]*).*/\1/g'`
//    iface_list="$iface_list,eth$eth"
//    done

    var netList = config.getString(s"system.$configKey.cli.net.list")
    if (netList == "") {

      val netListCmd = s"$dstat --net --full --nocolor 1 0 | head -n 1 | tr -d '-' | sed 's/net\\///g'"
      val netListRes = shell !! s""" ssh $slave "$netListCmd" """

      val pattern = """.*eth([0-9]*).*""".r
      netList = "total," + netListRes.split("\\s+").map { case pattern(num) => s"eth$num"; case s => s }.reduce((s1, s2) => s"$s1,$s2")
    }

    var diskList = config.getString(s"system.$configKey.cli.disk.list")
    if (diskList == "") {
      val diskListCmd = s"$dstat --disk --full --nocolor --noupdate 1 0 | head -n 1 | sed 's/ /\\n/g' | sed -r 's/.*dsk\\/([^-]*).*/\\1/g' | tr '\\n' ' '"
      val diskListRes = shell !! s""" ssh $slave "$diskListCmd" """
      diskList = "total," + diskListRes.split("\\s+").reduce((s1, s2) => s"$s1,$s2")
    }

    s"--epoch --cpu -C $cpuList --mem --net -N $netList --disk -D $diskList --noheaders --nocolor"
  }


  object Closeable {
    class Guard[A <: AutoCloseable](closeable: A) {
      def on[B](block: A => B) = {
        var t: Throwable = null
        try {
          block(closeable)
        } catch {
          case e: Exception => t = e; throw t
        } finally {
          if (closeable != null) {
            if (t != null) {
              try {
                closeable.close()
              } catch {
                case e: Throwable => t.addSuppressed(e)
              }
            } else {
              closeable.close()
            }
          }
        }
      }
    }

    def guard[A <: AutoCloseable](closable: A) = new Guard[A](closable)
  }
}
