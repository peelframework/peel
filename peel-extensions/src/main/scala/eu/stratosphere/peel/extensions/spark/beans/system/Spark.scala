package eu.stratosphere.peel.extensions.spark.beans.system

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigException
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

/** Wrapper Class for Spark
  *
  * Implements Spark as a [[eu.stratosphere.peel.core.beans.system.System System]] class and provides setup and teardown methods.
  *
  *
  * @param version Version of the system (e.g. "7.1")
  * @param lifespan [[eu.stratosphere.peel.core.beans.system.Lifespan Lifespan]] of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Spark(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("spark", version, lifespan, dependencies, mc) {

  /**
   * Parses the configuration and generates the Spark configuration files (slaves, spark-env.sh, spark-defaults.conf)
   * from the moustache templates
   *
   * @return [[eu.stratosphere.peel.core.config.SystemConfig]] for the configured system
   */
  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.spark.path.config")
    //rename template files - strip the .template
    // TODO: this needs to be done only on demand
    shell ! s"""if [ ! -e "$conf/spark-env.sh" ]; then mv "$conf/spark-env.sh.template" "$conf/spark-env.sh"; fi """
    shell ! s"""if [ ! -e "$conf/spark-defaults.conf" ]; then mv "$conf/spark-defaults.conf.template" "$conf/spark-defaults.conf"; fi """
    List(
      SystemConfig.Entry[Model.Hosts]("system.spark.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env]("system.spark.config.env", s"$conf/spark-env.sh", templatePath("conf/spark-env.sh"), mc),
      SystemConfig.Entry[Model.Site]("system.spark.config.defaults", s"$conf/spark-defaults.conf", templatePath("conf/spark-defaults.conf"), mc)
    )
  })

  /** Starts up the system and polls to check whether everything is up.
    *
    * This methods attempts to set up and start the system on all specified nodes.
    * If an attempt fails, we retry to start it ${system.spark.startup.max.attempts} times or until and shut down the system
    * after exceeding this limit.
    *
    * @throws SetUpTimeoutException If the system was not brought after ${system.spark.startup.max.attempts} or
    *                               {startup.pollingCounter} times {startup.pollingInterval} milliseconds.
    */
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
