package eu.stratosphere.peel.extensions.h2o.beans.system

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigException
import eu.stratosphere.peel.core.beans.system.Lifespan._
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

/** Wrapper for H2O
  *
  * Implements H2O as a [[eu.stratosphere.peel.core.beans.system.System System]] class and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "2.8.6")
  * @param lifespan [[eu.stratosphere.peel.core.beans.system.Lifespan Lifespan]] of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  *
  *
  */
class H2O(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("h2o", version, lifespan, dependencies, mc) {

  override def configuration() = SystemConfig(config, {
    val home = config.getString("system.h2o.path.home")
    List(
      SystemConfig.Entry[Model.HostsWithPort]("system.h2o.config", s"$home/flatfile", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Yaml]("system.h2o.config.cli", s"$home/conf", templatePath("conf/h2o-conf"), mc)
    )
  })

  override protected def start(): Unit = {
    val user = config.getString("system.h2o.user")
    val tmpDir =config.getString("system.h2o.config.cli.tmp.dir")
    val dataPort = config.getInt("system.h2o.config.cli.dataport")
    val memory = config.getString("system.h2o.config.cli.memory.per-node")
    val nthreads = config.getInt("system.h2o.config.cli.parallelism.per-node")
    val home = config.getString("system.h2o.path.home")

    // check if tmp dir exists and create if not
    try {

      for (dataNode <- config.getStringList("system.h2o.config.slaves").asScala) {
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
        val totl = config.getStringList("system.h2o.config.slaves").size()
        val init = 0 // H2O doesn't reset the log on startup

        for (dataNode <- config.getStringList("system.h2o.config.slaves").asScala) {
            shell ! s""" ssh $user@$dataNode "java -Xmx$memory -cp $home/h2odriver.jar water.H2OApp -flatfile $home/flatfile -nthreads $nthreads -ice_root $tmpDir -port $dataPort > /dev/null &" """
        }
        logger.info("Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.h2o.startup.polling.counter")
        val pollingNode = config.getStringList("system.h2o.config.slaves").get(0)
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.h2o.startup.polling.interval"))
          // get new values
          try {
            curr = Integer.parseInt((shell !! s""" wget -qO- $user@$pollingNode:$dataPort/3/Cloud.json | grep -Eo 'cloud_size":[0-9]+,' | grep -Eo '[0-9]+' """).stripLineEnd)
          } catch {
            case _ : Throwable => ;
          }
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.h2o.startup.max.attempts")) {
            stop()
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    val user = config.getString("system.h2o.user")
    for (dataNode <- config.getStringList("system.h2o.config.slaves").asScala) {
      shell ! s""" ssh $user@$dataNode "ps -ef | grep h2odriver.jar | grep -v grep | awk '{print \\$$2}' | xargs kill" """
    }
    isUp = false
  }

  def isRunning = {
    val pollingNode = config.getStringList("system.h2o.config.slaves").get(0)
    val user = config.getString("system.h2o.user")
    (shell ! s""" ssh $user@$pollingNode "ps -ef | grep h2odriver.jar | grep -v grep " """) == 0
  }
}