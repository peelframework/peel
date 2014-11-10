package eu.stratosphere.peel.extensions.zookeeper.beans.system

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.System
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._


class Zookeeper(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("zookeeper", version, lifespan, dependencies, mc) {

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
    shell ! s""" ssh $user@${s.host} ${config.getString(s"system.$configKey.path.home")}/bin/zkServer.sh start """
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
    val serverConfigs = config.getConfig(s"system.$configKey.config.server").entrySet().asScala.map(v => v.getValue.unwrapped().toString)
    // match and return valid server configs
    serverConfigs.collect({
      case Zookeeper.ServerConf(host, quorumPort, leaderPort) => Zookeeper.Server(host, quorumPort.toInt, leaderPort.toInt)
    })
  }
}

object Zookeeper {

  val ServerConf = "([\\w\\-\\_\\.]+):(\\d+):(\\d+)".r

  case class Server(host: String, quorumPort: Int, leaderPort: Int)

  class Cli(home: String, serverHost: String, serverPort: Int) {

    def !(cmd: String) = shell ! s"$home/bin/zkCli.sh -server $serverHost:$serverPort $cmd"

    def !!(cmd: String) = shell !! s"$home/bin/zkCli.sh -server $serverHost:$serverPort $cmd"
  }

}
