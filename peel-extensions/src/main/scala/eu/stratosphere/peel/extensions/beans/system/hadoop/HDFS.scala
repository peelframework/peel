package eu.stratosphere.peel.extensions.beans.system.hadoop

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{FileSystem, SetUpTimeoutException, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

class HDFS(version: String, lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("hdfs", version, lifespan, dependencies, mc) with FileSystem {

  override val configKey = "hadoop"

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def configuration() = SystemConfig(config, {
    val conf = config.getString("system.hadoop.path.config")
    List(
      SystemConfig.Entry[Model.Hosts]("system.hadoop.config.masters", s"$conf/masters", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Hosts]("system.hadoop.config.slaves", s"$conf/slaves", templatePath("conf/hosts"), mc),
      SystemConfig.Entry[Model.Env]("system.hadoop.config.env", s"$conf/hadoop-env.sh", templatePath("conf/hadoop-env.sh"), mc),
      SystemConfig.Entry[Model.Site]("system.hadoop.config.core", s"$conf/core-site.xml", templatePath("conf/site.xml"), mc),
      SystemConfig.Entry[Model.Site]("system.hadoop.config.hdfs", s"$conf/hdfs-site.xml", templatePath("conf/site.xml"), mc)
    )
  })

  /**
   * Checks if all datanodes have connected and the system is out of safemode.
   */
  override protected def start(): Unit = {
    if (config.getBoolean("system.hadoop.format")) format()

    val user = config.getString("system.hadoop.user")
    val logDir = config.getString("system.hadoop.path.log")
    val hostname = config.getString("app.hostname")

    var failedStartUpAttempts = 0
    while (!isUp) {
      try {
        val totl = config.getStringList("system.hadoop.config.slaves").size()
        var init = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.log | grep 'registerDatanode:' | wc -l""").trim())

        shell ! s"${config.getString("system.hadoop.path.home")}/bin/start-dfs.sh"
        logger.info(s"Waiting for nodes to connect")

        var curr = init
        var safe = !(shell !! s"${config.getString("system.hadoop.path.home")}/bin/hadoop dfsadmin -safemode get").toLowerCase.contains("off")
        var cntr = config.getInt("system.hadoop.startup.polling.counter")
        while (curr - init < totl || safe) {
          logger.info(s"Connected ${curr - init} from $totl nodes, safemode is ${if (safe) "ON" else "OFF"}")
          // wait a bit
          Thread.sleep(config.getInt("system.hadoop.startup.polling.interval"))
          // get new values
          curr = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.log | grep 'registerDatanode:' | wc -l""").trim())
          safe = !(shell !! s"${config.getString("system.hadoop.path.home")}/bin/hadoop dfsadmin -safemode get").toLowerCase.contains("off")
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
            shell ! s"${config.getString("system.hadoop.path.home")}/bin/stop-dfs.sh"
            logger.info(s"Could not bring system '$toString' up in time, trying again...")
          } else {
            throw e
          }
      }
    }
  }

  override protected def stop() = {
    shell ! s"${config.getString("system.hadoop.path.home")}/bin/stop-dfs.sh"
    if (config.getBoolean("system.hadoop.format")) format()
    isUp = false
  }

  // ---------------------------------------------------
  // FileSystem.
  // ---------------------------------------------------

  override def exists(path: String) = {
    val hadoopHome = config.getString("system.hadoop.path.home")
    (shell !! s"""if $hadoopHome/bin/hadoop fs -test -e "$path" ; then echo "YES" ; else echo "NO"; fi""").trim() == "YES"
  }

  override def rmr(path: String, skipTrash: Boolean = true) = {
    val hadoopHome = config.getString("system.hadoop.path.home")
    if (skipTrash)
      shell ! s"""$hadoopHome/bin/hadoop fs -rmr -skipTrash "$path" """
    else
      shell ! s"""$hadoopHome/bin/hadoop fs -rmr "$path" """
  }

  override def copyFromLocal(src: String, dst: String) = {
    val hadoopHome = config.getString("system.hadoop.path.home")
    if (src.endsWith(".gz"))
      shell ! s"""gunzip -c \"$src\" | $hadoopHome/bin/hadoop fs -put - \"$dst\" """
    else
      shell ! s"""$hadoopHome/bin/hadoop fs -copyFromLocal "$src" "$dst" """
  }

  // ---------------------------------------------------
  // Helper methods.
  // ---------------------------------------------------

  private def format() = {
    val user = config.getString("system.hadoop.user")
    val nameDir = config.getString("system.hadoop.config.hdfs.dfs.name.dir")

    logger.info(s"Formatting namenode")
    shell ! "echo 'Y' | %s/bin/hadoop namenode -format".format(config.getString("system.hadoop.path.home"))

    logger.info(s"Fixing data directories")
    for (dataNode <- config.getStringList("system.hadoop.config.slaves").asScala) {
      for (dataDir <- config.getString("system.hadoop.config.hdfs.dfs.data.dir").split(',')) {
        logger.info(s"Initializing data directory $dataDir at datanode $dataNode")
        shell ! s""" ssh $user@$dataNode "rm -Rf $dataDir/current" """
        shell ! s""" ssh $user@$dataNode "mkdir -p $dataDir/current" """
        logger.info(s"Copying namenode's VERSION file to datanode $dataNode")
        shell ! s""" scp $nameDir/current/VERSION $user@$dataNode:$dataDir/current/VERSION.backup """
        logger.info(s"Adapting VERSION file on datanode $dataNode")
        shell ! s""" ssh $user@$dataNode "cat $dataDir/current/VERSION.backup | sed '3 i storageID=' | sed 's/storageType=NAME_NODE/storageType=DATA_NODE/g'" > $dataDir/current/VERSION """
        shell ! s""" ssh $user@$dataNode "rm -Rf $dataDir/current/VERSION.backup" """
      }
    }
  }
}
