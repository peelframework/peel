package eu.stratosphere.peel.extensions.beans.system.hadoop

import java.nio.file.{Files, Paths}

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.System
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import eu.stratosphere.peel.core.util.shell

import scala.collection.JavaConverters._

class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends System("hdfs", lifespan, dependencies, mc) {

  // ---------------------------------------------------
  // System.
  // ---------------------------------------------------

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'")

    if (config.hasPath("system.hadoop.path.archive")) {
      if (!Files.exists(Paths.get(config.getString("system.hadoop.path.home")))) {
        logger.info(s"Extracting archive ${config.getString("system.hadoop.path.archive.src")} to ${config.getString("system.hadoop.path.archive.dst")}")
        shell.untar(config.getString("system.hadoop.path.archive.src"), config.getString("system.hadoop.path.archive.dst"))

        logger.info(s"Changing owner of ${config.getString("system.hadoop.path.home")} to ${config.getString("system.hadoop.user")}:${config.getString("system.hadoop.group")}")
        shell ! "chown -R %s:%s %s".format(
          config.getString("system.hadoop.user"),
          config.getString("system.hadoop.group"),
          config.getString("system.hadoop.path.home"))
      }
    }

    configuration().update()

    if (config.getBoolean("system.hadoop.format")) format()

    shell ! s"${config.getString("system.hadoop.path.home")}/bin/start-dfs.sh"
    logger.info(s"Waiting for safemode to exit")
    waitUntilAllDataNodesRunning()
    logger.info(s"System '$toString' is now running")
  }

  override def tearDown(): Unit = {
    logger.info(s"Tearing down system '$toString'")

    shell ! s"${config.getString("system.hadoop.path.home")}/bin/stop-dfs.sh"

    if (config.getBoolean("system.hadoop.format")) format()
  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")

    val c = configuration()
    if (c.hasChanged) {
      logger.info(s"Configuration changed, restarting '$toString'...")
      shell ! s"${config.getString("system.hadoop.path.home")}/bin/stop-dfs.sh"

      if (config.getBoolean("system.hadoop.format")) format()

      c.update()

      if (config.getBoolean("system.hadoop.format")) format()

      shell ! s"${config.getString("system.hadoop.path.home")}/bin/start-dfs.sh"
      logger.info(s"Waiting for safemode to exit")
      waitUntilAllDataNodesRunning()
      logger.info(s"System '$toString' is now running")
    }
  }

  override def configuration() = SystemConfig(config, List(
    SystemConfig.Entry[Model.Hosts]("system.hadoop.config.masters",
      "%s/masters".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Hosts]("system.hadoop.config.slaves",
      "%s/slaves".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Env]("system.hadoop.config.env",
      "%s/hadoop-env.sh".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/hadoop-env.sh.mustache", mc),
    SystemConfig.Entry[Model.Site]("system.hadoop.config.core",
      "%s/core-site.xml".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/site.xml.mustache", mc),
    SystemConfig.Entry[Model.Site]("system.hadoop.config.hdfs",
      "%s/hdfs-site.xml".format(config.getString("system.hadoop.path.config")),
      "/templates/hadoop/conf/site.xml.mustache", mc)
  ))

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

  /**
   * Checks if all datanodes have connected and the system is out of safemode.
   */
  private def waitUntilAllDataNodesRunning(): Unit = {
    val user = config.getString("system.hadoop.user")
    val logDir = config.getString("system.hadoop.path.log")
    val hostname = config.getString("app.hostname")

    val totl = config.getStringList("system.hadoop.config.slaves").size()
    val init = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.log | grep 'registerDatanode:' | wc -l""").trim())
    var curr = init
    var safe = !(shell !! s"${config.getString("system.hadoop.path.home")}/bin/hadoop dfsadmin -safemode get").toLowerCase.contains("off")
    var cntr = pollingCounter

    while (curr - init < totl || safe) {
      logger.info(s"Connected ${curr - init} from $totl nodes, safemode is ${if (safe) "ON" else "OFF"}")
      // wait a bit
      Thread.sleep(pollingInterval)
      // get new values
      curr = Integer.parseInt((shell !! s"""cat $logDir/hadoop-$user-namenode-$hostname.log | grep 'registerDatanode:' | wc -l""").trim())
      safe = !(shell !! s"${config.getString("system.hadoop.path.home")}/bin/hadoop dfsadmin -safemode get").toLowerCase.contains("off")
      // timeout if counter goes below zero
      cntr = cntr - 1
      if (cntr < 0) throw new RuntimeException(s"Cannot start system '$toString'; node connection timeout at system ")
    } // TODO: don't loop to infinity
  }
}
