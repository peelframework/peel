package eu.stratosphere.peel.extensions.spark.beans.job

import eu.stratosphere.peel.core.beans.system.Job
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.spark.beans.system.Spark

class SparkJob(command: String, runner: Spark, timeout: Long) extends Job(command, runner, timeout) {

  def this(command: String, runner: Spark) = this(command, runner, 600)

  def runJob() = {
    this ! resolve(command)
  }

  def cancelJob() = {}

  private def !(command: String) = {
    val master = config.getString("system.spark.config.defaults.spark.master")
    shell ! s"${config.getString("system.spark.path.home")}/bin/spark-submit --master $master $command"
  }
}
