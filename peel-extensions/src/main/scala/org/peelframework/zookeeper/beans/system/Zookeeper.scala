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

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Site](s"system.$configKey.config", s"$conf/zoo.cfg", templatePath("conf/zoo.cfg"), mc)
    )
  })

  override def start(): Unit = if (!isUp) {
    this.servers.foreach(start)
    isUp = true
  }

  override def stop(): Unit = this.servers.foreach(stop)

  def isRunning = this.servers.forall(s => isRunning(s))

  def cli = new Zookeeper.Cli(config.getString(s"system.$configKey.path.home"), servers.head.host, config.getInt(s"system.$configKey.config.clientPort"))

  private def start(s: Zookeeper.Server) = {
    logger.info(s"Starting zookeeper at ${s.host}:${s.leaderPort}:${s.quorumPort}")
    val user = config.getString(s"system.$configKey.user")
    shell !
      s"""
        |ssh -t -t "$user@${s.host}" << SSHEND
        |  ${config.getString(s"system.$configKey.path.home")}/bin/zkServer.sh start
        |  echo ${s.id} > ${config.getString(s"system.$configKey.config.dataDir")}/myid
        |  exit
        |SSHEND
      """.stripMargin.trim
  }

  private def stop(s: Zookeeper.Server) = {
    logger.info(s"Stopping zookeeper at ${s.host}:${s.leaderPort}:${s.quorumPort}")
    val user = config.getString(s"system.$configKey.user")
    shell ! s""" ssh $user@${s.host} ${config.getString(s"system.$configKey.path.home")}/bin/zkServer.sh stop """
  }

  private def isRunning(s: Zookeeper.Server) = {
    val user = config.getString(s"system.$configKey.user")
    val pidFile = s"${config.getString(s"system.$configKey.config.dataDir")}/zookeeper_server.pid"

    val output = shell !!
      s"""
        |ssh -t -t "$user@${s.host}" << SSHEND
        | if [ -f $pidFile ]; then
        |   if kill -0 `cat $pidFile` > /dev/null 2>&1; then
        |     echo TRUE
        |   else
        |     echo FALSE
        |   fi
        | fi
        | exit
        |SSHEND
       """.stripMargin.trim

    output.split("\\n").count(_.trim == "TRUE") > 0
  }

  private def servers = {
    // grab servers from config
    val serverConfigs = config.getConfig(s"system.$configKey.config.server").entrySet().asScala.map(v => v.getKey.substring(1, v.getKey.length() - 1) + ":" + v.getValue.unwrapped().toString)
    // match and return valid server configs
    serverConfigs.collect({
      case Zookeeper.ServerConf(id, host, quorumPort, leaderPort) => Zookeeper.Server(id.toInt, host, quorumPort.toInt, leaderPort.toInt)
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
