package eu.stratosphere.peel.extensions.aura.beans.system

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.zookeeper.beans.system.Zookeeper

class Aura(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("aura", version, lifespan, dependencies, mc) {

  // make sure a Zookeeper dependency is provided
  if (dependencies.collectFirst({
    case zk: Zookeeper => zk
  }).isEmpty) throw new RuntimeException(s"Missing Zookeeper system dependency for Aura bean $beanName")

  // save a reference to the Zookeeper system
  val zookeeper = dependencies.collectFirst({
    case zk: Zookeeper => zk
  }).get

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.HOCON](s"system.$configKey.config.common", s"$conf/aura.conf", templatePath("conf/aura.conf"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.slaves").size()
        val init = 0 // aura resets the job manager log on startup

        if (!zookeeper.isRunning) {
          logger.info("Zookeeper still not running, trying to start again")
          zookeeper.start()
        }

        // clean up zookeeper entries from an old setup
        zookeeper.cli ! "rmr /aura"

        shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/start-cluster.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          curr = (zookeeper.cli !! "get /aura/taskmanagers 2>&1 | grep 'numChildren = ' | sed 's/numChildren = //'").trim.toInt
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/stop-cluster.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString(s"system.$configKey.path.home")}/bin/stop-cluster.sh"
    isUp = false
  }

  def isRunning = {
    (shell ! s""" ps -ef | grep 'aura' | grep 'java' | grep 'wm' | grep -v 'grep' """) == 0 // TODO: fix using PID
  }
}
