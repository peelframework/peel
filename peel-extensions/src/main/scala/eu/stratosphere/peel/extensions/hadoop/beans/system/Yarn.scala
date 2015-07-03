package eu.stratosphere.peel.extensions.hadoop.beans.system

import java.net.HttpURLConnection

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigException
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

/** Wrapper class for Yarn
  *
  * Implements Yarn as a [[eu.stratosphere.peel.core.beans.system.System System]] class and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param lifespan [[eu.stratosphere.peel.core.beans.system.Lifespan Lifespan]] of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Yarn(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("yarn", version, lifespan, dependencies, mc) {

  override val configKey = "hadoop-2" //hadoop-2?

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString(s"system.$configKey.path.config")
    List(
      SystemConfig.Entry[Model.Hosts](s"system.$configKey.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env](s"system.$configKey.config.env", s"$conf/hadoop-env.sh", templatePath("conf/hadoop-env.sh"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.core", s"$conf/core-site.xml", templatePath("conf/site.xml"), mc),
      SystemConfig.Entry[Model.Site](s"system.$configKey.config.yarn", s"$conf/yarn-site.xml", templatePath("conf/site.xml"), mc)
    )
  })

  override def start(): Unit = {
    val user = config.getString(s"system.$configKey.user")
    val logDir = config.getString(s"system.$configKey.path.log")

    // check if tmp dir exists and create if not
    try {
      val tmpDir = config.getString(s"system.$configKey.config.defaults.spark.local.dir")

      for (nodemanager <- config.getStringList(s"system.$configKey.config.slaves").asScala) {
        logger.info(s"Initializing tmp directory $tmpDir at taskmanager node $nodemanager")
        shell ! s""" ssh $user@$nodemanager "rm -Rf $tmpDir" """
        shell ! s""" ssh $user@$nodemanager "mkdir -p $tmpDir" """
      }
    } catch {
      case _: ConfigException => // ignore not set explicitly, java default is taken
    }

    var failedStartUpAttempts = 0
    while(!isUp) {
      try {
        val total = config.getStringList(s"system.$configKey.config.slaves").size()
        // yarn does not reset the resourcemanagers log at startup
        val init = Integer.parseInt((shell !! s"""cat $logDir/yarn-$user-resourcemanager-*.log | grep 'registered with capability:' | wc -l""").trim())

        shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/yarn-daemon.sh start resourcemanager"
        shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/yarn-daemon.sh start nodemanager"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt(s"system.$configKey.startup.polling.counter")
        while (curr - init < total) {
          logger.info(s"Connected ${curr - init} from $total nodes")
          // wait a bit
          Thread.sleep(config.getInt(s"system.$configKey.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/yarn-$user-resourcemanager-*.log | grep 'registered with capability:' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        logger.info(s"Connected ${curr - init} from $total nodes")
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt(s"system.$configKey.startup.max.attempts")) {
            stop()
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override def stop(): Unit = {
    shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/yarn-daemon.sh stop resourcemanager"
    shell ! s"${config.getString(s"system.$configKey.path.home")}/sbin/yarn-daemon.sh stop nodemanager"

    isUp = false
  }

  def isRunning = {
    (shell ! s"""ps -ef | grep 'yarn' | grep 'java' | grep 'resourcemanager' | grep -v 'grep' """) == 0 // TODO: fix using PID
  }
}