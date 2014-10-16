package eu.stratosphere.peel.extensions.flink.beans.system

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

class Flink(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("flink", version, lifespan, dependencies, mc) {

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.flink.path.config")
    List(
      SystemConfig.Entry[Model.Hosts]("system.flink.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml]("system.flink.config.yaml", s"$conf/flink-conf.yaml", templatePath("conf/flink-conf.yaml"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString("system.flink.user")
    val logDir = config.getString("system.flink.path.log")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList(s"system.$configKey.config.slaves").size()
        val init = 0 // Flink resets the job manager log on startup

        shell ! s"${config.getString("system.flink.path.home")}/bin/start-cluster.sh"
        shell ! s"${config.getString("system.flink.path.home")}/bin/start-webclient.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.flink.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.flink.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/flink-$user-jobmanager-*.log | grep 'Creating instance' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.flink.startup.max.attempts")) {
            shell ! s"${config.getString("system.flink.path.home")}/bin/stop-cluster.sh"
            shell ! s"${config.getString("system.flink.path.home")}/bin/stop-webclient.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString("system.flink.path.home")}/bin/stop-cluster.sh"
    shell ! s"${config.getString("system.flink.path.home")}/bin/stop-webclient.sh"
    isUp = false
  }

  def isRunning = {
    (shell ! s"""ps -ef | grep 'flink' | grep 'java' | grep 'jobmanager' | grep -v 'grep' """) == 0 // TODO: fix using PID
  }
}
