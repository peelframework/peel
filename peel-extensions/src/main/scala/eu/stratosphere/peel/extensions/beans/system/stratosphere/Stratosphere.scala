package eu.stratosphere.peel.extensions.beans.system.stratosphere

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

class Stratosphere(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("stratosphere", version, lifespan, dependencies, mc) {

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.stratosphere.path.config")
    List(
      SystemConfig.Entry[Model.Hosts]("system.stratosphere.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml]("system.stratosphere.config.yaml", s"$conf/stratosphere-conf.yaml", templatePath("conf/stratosphere-conf.yaml"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString("system.stratosphere.user")
    val logDir = config.getString("system.stratosphere.path.log")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList("system.hadoop.config.slaves").size()
        val init = 0 // Stratosphere resets the job manager log on startup

        shell ! s"${config.getString("system.stratosphere.path.home")}/bin/start-cluster.sh"
        shell ! s"${config.getString("system.stratosphere.path.home")}/bin/start-webclient.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.stratosphere.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.stratosphere.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/stratosphere-$user-jobmanager-*.log | grep 'Creating instance' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.stratosphere.startup.max.attempts")) {
            shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-cluster.sh"
            shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-webclient.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-cluster.sh"
    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-webclient.sh"
    isUp = false
  }
}
