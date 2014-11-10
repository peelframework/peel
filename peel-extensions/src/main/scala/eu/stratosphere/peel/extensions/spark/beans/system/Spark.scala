package eu.stratosphere.peel.extensions.spark.beans.system

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigException
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{Lifespan, SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

class Spark(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("spark", version, lifespan, dependencies, mc) {

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.spark.path.config")
    //rename template files - strip the .template
    shell ! s"mv $conf/spark-env.sh.template $conf/spark-env.sh"
    shell ! s"mv $conf/spark-defaults.conf.template $conf/spark-defaults.conf"
    List(
      SystemConfig.Entry[Model.Hosts]("system.spark.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env]("system.spark.config.env", s"$conf/spark-env.sh",templatePath("conf/spark-env.sh"), mc),
      SystemConfig.Entry[Model.Site]("system.spark.config.defaults", s"$conf/spark-defaults.conf", templatePath("conf/spark-defaults.conf"), mc)
    )
  })

  override def start(): Unit = {
    val user = config.getString("system.spark.user")
    val logDir = config.getString("system.spark.path.log")

    // check if tmp dir exists and create if not
    try {
      val tmpDir = config.getString("system.spark.config.defaults.spark.local.dir")

      for (dataNode <- config.getStringList(s"system.spark.config.slaves").asScala) {
        logger.info(s"Initializing tmp directory $tmpDir at taskmanager node $dataNode")
        shell ! s""" ssh $user@$dataNode "rm -Rf $tmpDir" """
        shell ! s""" ssh $user@$dataNode "mkdir -p $tmpDir" """
      }
    } catch {
      case _: ConfigException => // ignore not set explicitly, java default is taken
    }

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList("system.spark.config.slaves").size()
        val init = 0 // Spark resets the job manager log on startup

        shell ! s"${config.getString("system.spark.path.home")}/sbin/start-all.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.spark.startup.polling.counter")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.spark.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/spark-$user-org.apache.spark.deploy.master.Master-*.out | grep 'Registering worker' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.spark.startup.max.attempts")) {
            shell ! s"${config.getString("system.spark.path.home")}/sbin/stop-all.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override def stop(): Unit = {
    shell ! s"${config.getString("system.spark.path.home")}/sbin/stop-all.sh"
    isUp = false
  }

  def isRunning = {
    (shell ! s"""ps -ef | grep 'spark' | grep 'java' | grep 'Master' | grep -v 'grep' """) == 0 // TODO: fix using PID
  }

}
