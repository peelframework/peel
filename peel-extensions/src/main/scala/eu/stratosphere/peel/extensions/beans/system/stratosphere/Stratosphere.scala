package eu.stratosphere.peel.extensions.beans.system.stratosphere

import java.io.File
import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.peel.core.beans.system.{SetUpTimeoutException, ExperimentRunner, System}
import eu.stratosphere.peel.core.config.{Model, SystemConfig}
import java.nio.file.{Paths, Files}
import eu.stratosphere.peel.core.util.shell

class Stratosphere(lifespan: Lifespan, dependencies: Set[System] = Set(), mc: Mustache.Compiler) extends ExperimentRunner("stratosphere", lifespan, dependencies, mc) {

  override def setUp(): Unit = {
    logger.info(s"Starting system '$toString'")

    if (config.hasPath("system.stratosphere.path.archive")) {
      if (!Files.exists(Paths.get(config.getString("system.stratosphere.path.home")))) {
        logger.info(s"Extracting archive ${config.getString("system.stratosphere.path.archive.src")} to ${config.getString("system.stratosphere.path.archive.dst")}")
        shell.untar(config.getString("system.stratosphere.path.archive.src"), config.getString("system.stratosphere.path.archive.dst"))

        logger.info(s"Changing owner of ${config.getString("system.stratosphere.path.home")} to ${config.getString("system.stratosphere.user")}:${config.getString("system.stratosphere.group")}")
        shell ! "chown -R %s:%s %s".format(
          config.getString("system.stratosphere.user"),
          config.getString("system.stratosphere.group"),
          config.getString("system.stratosphere.path.home"))
      }
    }

    configuration().update()

    var failedStartUpAttempts = 0
    var systemIsUp = false
    while (!systemIsUp) {
      try {
        startAndWait()
        systemIsUp = true
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

    logger.info(s"System '$toString' is now running")
  }

  override def configuration() = SystemConfig(config, List(
    SystemConfig.Entry[Model.Hosts]("system.stratosphere.config.slaves",
      "%s/slaves".format(config.getString("system.stratosphere.path.config")),
      "/templates/stratosphere/conf/hosts.mustache", mc),
    SystemConfig.Entry[Model.Yaml]("system.stratosphere.config.yaml",
      "%s/stratosphere-conf.yaml".format(config.getString("system.stratosphere.path.config")),
      "/templates/stratosphere/conf/stratosphere-conf.yaml.mustache", mc)
  ))

  override def run(job: String, input: List[File], output: File) = {
    logger.info("Running Stratosphere Job...")
    //Shell ! home + "bin/stratosphere run %s %s %s".format(job, input.mkString(" "), output)
  }


  override def tearDown(): Unit = {
    logger.info(s"Tearing down system '$toString'")

    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-cluster.sh"
    shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-webclient.sh"
  }

  override def update(): Unit = {
    logger.info(s"Checking system configuration of '$toString'")

    val c = configuration()
    if (c.hasChanged) {
      logger.info(s"Configuration changed, restarting '$toString'...")
      shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-cluster.sh"
      shell ! s"${config.getString("system.stratosphere.path.home")}/bin/stop-webclient.sh"

      c.update()

      var failedStartUpAttempts = 0
      var systemIsUp = false
      while (!systemIsUp) {
        try {
          startAndWait()
          systemIsUp = true
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

      logger.info(s"System '$toString' is now running")
    }
  }

  override protected def startAndWait(): Unit = {
    val user = config.getString("system.stratosphere.user")
    val logDir = config.getString("system.stratosphere.path.log")

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
  }
}
