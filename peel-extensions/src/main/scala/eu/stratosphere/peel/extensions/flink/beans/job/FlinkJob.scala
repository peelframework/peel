package eu.stratosphere.peel.extensions.flink.beans.job

import eu.stratosphere.peel.core.beans.system.Job
import eu.stratosphere.peel.core.util.shell
import eu.stratosphere.peel.extensions.flink.beans.system.Flink

/** Wrapper for a Job in Flink.
  *
  * The FlinkJob follows the same procedure as launching a Flink application on the command line and uses
  * `./bin/flink` internally.
  *
  * @example Usage in a Spring fixture-file:
  *
  *  {{{
  *  <bean id="IdForMyBean" class="eu.stratosphere.peel.extensions.flink.beans.job.FlinkJob">
  *      <constructor-arg name="runner" ref="flink-0.9.0"/>
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
class FlinkJob(command: String, runner: Flink, timeout: Long) extends Job(command, runner, timeout) {

  def this(command: String, runner: Flink) = this(command, runner, 600)

  def runJob() = {
    this ! resolve(command)
  }

  def cancelJob() = {}

  private def !(command: String) = {
    shell ! s"${config.getString("system.flink.path.home")}/bin/flink run $command"
  }
}
