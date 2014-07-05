package eu.stratosphere.peel.extensions.beans.system.hadoop

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

class MapReduce(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("mapreduce", version, lifespan, dependencies, mc) {

  override val configKey = "hadoop"

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.stratosphere.path.config")
    List(
      SystemConfig.Entry[Model.Hosts]("system.hadoop.config.masters", s"$conf/masters", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Hosts]("system.hadoop.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env]("system.hadoop.config.env", s"$conf/hadoop-env.sh", templatePath("conf/hadoop-env.sh"), mc),
      SystemConfig.Entry[Model.Site]("system.hadoop.config.core", s"$conf/core-site.xml", templatePath("conf/site.xml"), mc),
      SystemConfig.Entry[Model.Site]("system.hadoop.config.mapred", s"$conf/mapred-site.xml", templatePath("conf/site.xml"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString("system.hadoop.user")
    val logDir = config.getString("system.hadoop.path.log")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList("system.hadoop.config.slaves").size()
        var init = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-jobtracker-*.log | grep 'Adding a new node:' | wc -l""").trim())

        shell ! s"${config.getString("system.hadoop.path.home")}/bin/start-mapred.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.hadoop.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.hadoop.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-jobtracker-*.log | grep 'Adding a new node:' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (curr - init < 0) init = 0 // protect against log reset on startup
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.hadoop.startup.max.attempts")) {
            shell ! s"${config.getString("system.hadoop.path.home")}/bin/stop-mapred.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString("system.hadoop.path.home")}/bin/stop-mapred.sh"
    isUp = false
  }
}
