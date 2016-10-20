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
package org.peelframework.zookeeper.beans.system

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan.Lifespan
import org.peelframework.core.beans.system.System
import org.peelframework.core.config.{Model, SystemConfig}
import org.peelframework.core.util.shell

import scala.collection.JavaConverters._

/** Wrapper class for Zookeper
  *
  * @param version Version of the system (e.g. "7.1")
  * @param configKey The system configuration resides under `system.\${configKey}`
  * @param lifespan `Lifespan` of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Zookeeper(
  version      : String,
  configKey    : String,
  lifespan     : Lifespan,
  dependencies : Set[System] = Set(),
  mc           : Mustache.Compiler) extends System("zookeeper", version, configKey, lifespan, dependencies, mc) {

  override def configuration() = SystemConfig(config, List(
    SystemConfig.Entry[Model.Site](s"system.$configKey.config", s"${c.conf}/zoo.cfg", templatePath("conf/zoo.cfg"), mc)
  ))

  override def start(): Unit = if (!isUp) {
    c.servers.foreach(start)
    isUp = true
  }

  override def stop(): Unit =
    c.servers.foreach(stop)

  def isRunning =
    c.servers.forall(s => isRunning(s))

  def cli =
    new Zookeeper.Cli(c.home, c.servers.head.host, c.clientPort)

  private def start(s: Zookeeper.Server) = {
    logger.info(s"Starting zookeeper at ${s.host}:${s.leaderPort}:${s.quorumPort}")
    val user = c.user
    shell !
      s"""
         |ssh -t -t "$user@${s.host}" << SSHEND
         |  rm -Rf ${c.dataDir}
         |  mkdir -p ${c.dataDir}
         |  ${c.home}/bin/zkServer.sh start
         |  echo ${s.id} > ${c.dataDir}/myid
         |  exit
         |SSHEND
      """.stripMargin.trim
  }

  private def stop(s: Zookeeper.Server) = {
    logger.info(s"Stopping zookeeper at ${s.host}:${s.leaderPort}:${s.quorumPort}")
    shell !
      s"""
         |ssh -t -t "${c.user}@${s.host}" << SSHEND
         |  ${c.home}/bin/zkServer.sh stop
         |  rm -Rf ${c.dataDir}
         |  exit
         |SSHEND
      """.stripMargin
  }

  private def isRunning(s: Zookeeper.Server) = {
    val pidFile = s"${c.dataDir}/zookeeper_server.pid"
    (shell !! s""" ssh ${c.user}@${s.host} "ps -p `cat $pidFile` >/dev/null 2>&1; echo $$?" """).stripLineEnd.toInt == 0
  }

  // config parameter shorthands

  private object c {

    def conf =
      config.getString(s"system.$configKey.path.config")

    def user =
      config.getString(s"system.$configKey.user")

    def home =
      config.getString(s"system.$configKey.path.home")

    def clientPort =
      config.getInt(s"system.$configKey.config.clientPort")

    def dataDir =
      config.getString(s"system.$configKey.config.dataDir")

    def servers =
      config.getConfig(s"system.$configKey.config.server").entrySet().asScala
        .map(v => v.getKey.substring(1, v.getKey.length() - 1) + ":" + v.getValue.unwrapped().toString)
        .collect({
          case Zookeeper.ServerConf(id, host, quorumPort, leaderPort) =>
            Zookeeper.Server(id.toInt, host, quorumPort.toInt, leaderPort.toInt)
        })
  }
}

object Zookeeper {

  val ServerConf = "(\\d+):([\\w\\-\\_\\.]+):(\\d+):(\\d+)".r

  case class Server(id: Int, host: String, quorumPort: Int, leaderPort: Int)

  class Cli(home: String, serverHost: String, serverPort: Int) {

    def !(cmd: String) = shell ! s"$home/bin/zkCli.sh -server $serverHost:$serverPort $cmd"

    def !!(cmd: String) = shell !! s"$home/bin/zkCli.sh -server $serverHost:$serverPort $cmd"
  }

}
