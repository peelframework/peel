package eu.stratosphere.peel.extensions.cassandra.beans.system

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigException
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

/** Wrapper class for Cassandra
  *
  * Implements Cassandra as a [[eu.stratosphere.peel.core.beans.system.System System]] class and provides setup and teardown methods.
  *
  * @param version Version of the system (e.g. "7.1")
  * @param lifespan [[eu.stratosphere.peel.core.beans.system.Lifespan Lifespan]] of the system
  * @param dependencies Set of dependencies that this system needs
  * @param mc The moustache compiler to compile the templates that are used to generate property files for the system
  */
class Cassandra(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("cassandra", version, lifespan, dependencies, mc) {


  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.cassandra.path.config")
    List(
      SystemConfig.Entry[Model.Env]("system.cassandra.config.env", s"$conf/cassandra-env.sh", templatePath("conf/cassandra-env.sh"), mc),
      SystemConfig.Entry[Model.Yaml]("system.cassandra.config.yaml", s"$conf/cassandra.yaml", templatePath("conf/cassandra.yaml"), mc),
      SystemConfig.Entry[Model.Env]("system.cassandra.config.in", s"$conf/../bin/cassandra.in.sh", templatePath("conf/cassandra.in.sh"), mc)
    )
  })


  override protected def start(): Unit = {

    val user = config.getString("system.cassandra.user")
    val logDir = config.getString("system.cassandra.path.log")
    val home = config.getString("system.cassandra.path.home")

    // remove the tmp directories
    try {
      val dataFile = config.getString("system.cassandra.config.yaml.data_file_directory")
      val commitlog = config.getString("system.cassandra.config.yaml.commitlog_directory")
      val savedCaches = config.getString("system.cassandra.config.yaml.saved_caches_directory")
      for (dataNode <- config.getStringList(s"system.cassandra.config.slaves").asScala) {
        logger.info(s"Initializing tmp directory $dataFile, $commitlog, $savedCaches at node $dataNode")
        shell ! s""" ssh $user@$dataNode "rm -Rf $dataFile $commitlog $savedCaches" """
        shell ! s""" ssh $user@$dataNode "mkdir $dataFile $commitlog $savedCaches" """

        //prepare pid directory
        shell ! s""" ssh $user@$dataNode "rm -Rf $home/pid" """
        shell ! s""" ssh $user@$dataNode "mkdir $home/pid" """
      }
    } catch {
      case _: ConfigException => // ignore not set explicitly, java default is taken
    }

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList("system.cassandra.config.slaves").size()
        val init = 0

        for (dataNode <- config.getStringList("system.cassandra.config.slaves").asScala) {
          shell ! s""" ssh $user@$dataNode "${config.getString("system.cassandra.path.home")}/bin/cassandra -Dcassandra.logdir=$logDir -p $home/pid/$dataNode  > /dev/null" """
        }
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var cntr = config.getInt("system.cassandra.startup.polling.counter")
        val pollingNode = config.getStringList("system.cassandra.config.slaves").get(0)
        val port = config.getString("system.cassandra.config.env.JMX_PORT")
        while (curr - init < totl) {
          logger.info(s"Connected ${curr - init} from $totl nodes")
          // wait a bit
          Thread.sleep(config.getInt("system.cassandra.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""${config.getString("system.cassandra.path.home")}/bin/nodetool -h $pollingNode -p $port status | grep 'UN' | wc -l""").trim())
          // timeout if counter goes below zero
          cntr = cntr - 1
          if (cntr < 0) throw new SetUpTimeoutException(s"Cannot start system '$toString'; node connection timeout at system ")
        }
        isUp = true
      } catch {
        case e: SetUpTimeoutException =>
          failedStartUpAttempts = failedStartUpAttempts + 1
          if (failedStartUpAttempts < config.getInt("system.cassandra.startup.max.attempts")) {
            stop()
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    val user = config.getString("system.cassandra.user")
    val home = config.getString("system.cassandra.path.home")

    for (dataNode <- config.getStringList("system.cassandra.config.slaves").asScala) {
      shell ! s""" ssh $user@$dataNode "cat $home/pid/$dataNode | xargs kill" """
    }
    isUp = false
  }

  /**
   * Checks if all datanodes are down using pid
   */
  def isRunning = {
    val user = config.getString("system.cassandra.user")
    val home = config.getString("system.cassandra.path.home")

    var checkRunning = false
    for (dataNode <- config.getStringList("system.cassandra.config.slaves").asScala) {
      if ((shell ! s""" ssh $user@$dataNode "cat $home/pid/$dataNode" """) == 0) {
        checkRunning = checkRunning || ((shell ! s""" ssh $user@$dataNode "cat $home/pid/$dataNode | xargs ps -p" """) == 0)
      }
    }
    checkRunning
  }

}
