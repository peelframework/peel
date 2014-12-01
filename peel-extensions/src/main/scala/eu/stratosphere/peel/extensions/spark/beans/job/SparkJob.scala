package eu.stratosphere.peel.extensions.spark.beans.job

import eu.stratosphere.peel.core.beans.system.Job
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.spark.beans.system.Spark

/** Wrapper for a Job in Spark.
  *
  * The SparkJob follows the same procedure as launching a Spark application on the command line and uses
  * `./bin/spark-submit` internally. More information on the available command-options can be found at
  * [[https://spark.apache.org/docs/latest/submitting-applications.html]]
  *
  * @example Usage in a Spring fixture-file:
  *
  *  {{{
  *  <bean id="IdForMyBean" class="eu.stratosphere.peel.extensions.spark.beans.job.SparkJob">
  *      <constructor-arg name="runner" ref="spark-1.1.0"/>
  *      <constructor-arg name="command">
  *          <value>
  *            --class eu.stratosphere.peel.MyExampleJob \
  *            ${app.path.someConfigVar}/peel-myexamples-1.0-SNAPSHOT.jar \
  *            ${system.spark.config.defaults.spark.master} ${system.default.config.parallelism.total} 100 \
  *            ${app.path.config}/inputfile.csv
  *          </value>
  *      </constructor-arg>
  *  </bean>
  *  }}}
  *
  *  @note The master is set through the spark-config file and does not have to be added through the command.
  *
 * @param command Command to run the job (as specified by the specific system)
 * @param runner System that this job is written for
 * @param timeout Timeout limit for the job. If the job does not finish within the given limit, it is canceled.
  *                Defaults to 600 seconds.
 */
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
