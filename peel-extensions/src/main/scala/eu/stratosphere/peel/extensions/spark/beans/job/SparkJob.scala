package eu.stratosphere.peel.extensions.spark.beans.job

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.system.Job
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.spark.beans.system.Spark

class SparkJob(command: String, runner: Spark, config: Config) extends Job(command, runner, config) {

  def runJob() = {
    this ! command
  }

  def cancelJob() = {}

  private def !(command: String) = {
    val master = config.getString("system.spark.config.defaults.spark.master")
    shell ! s"${config.getString("system.spark.path.home")}/bin/spark-submit --master $master $command"
  }

}
