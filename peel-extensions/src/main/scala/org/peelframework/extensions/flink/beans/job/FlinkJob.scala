package org.peelframework.extensions.flink.beans.job

import java.nio.file.Paths

import org.peelframework.core.beans.system.Job
import org.peelframework.core.util.shell
import org.peelframework.extensions.flink.beans.system.Flink
import org.peelframework.extensions.flink.beans.system.Flink

/** Wrapper for a Job in Flink.
  *
  * The FlinkJob follows the same procedure as launching a Flink application on the command line and uses
  * `./bin/flink` internally.
  *
  * @example Usage in a Spring XML context:
  *
  *  {{{
  *  <bean id="IdForMyBean" class="org.peelframework.extensions.flink.beans.job.FlinkJob">
  *      <constructor-arg name="runner" ref="flink-0.9.0"/>
  *      <constructor-arg name="command">
  *          <value>
  *            --class org.peelframework.MyDatagenJob \
  *            ${app.path.someConfigVar}/my-datagen-job.jar \
  *            ${system.default.config.parallelism.total} 100 \
  *            ${app.path.config}/inputfile.csv
  *          </value>
  *      </constructor-arg>
  *  </bean>
  *  }}}
  *
  * @param command Command to run the job (as specified by the specific system)
  * @param runner System that this job is written for
  * @param timeout Timeout limit for the job. If the job does not finish within the given limit, it is canceled.
  *                Defaults to 600 seconds.
  */
class FlinkJob(command: String, runner: Flink, timeout: Long) extends Job(command, runner, timeout) {

  def this(command: String, runner: Flink) = this(command, runner, 600)

  def runJob() = {
    ensureFolderIsWritable(Paths.get(s"$home"))
    this ! resolve(command)
  }

  def cancelJob() = {}

  private def !(command: String) = {
    shell ! s"${config.getString("system.flink.path.home")}/bin/flink run ${command.trim} >> $home/run.out 2>> $home/run.err"
  }
}
