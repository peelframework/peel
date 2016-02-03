/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.spark.beans.job

import java.nio.file.Paths

import org.peelframework.core.beans.system.Job
import org.peelframework.core.util.shell
import org.peelframework.spark.beans.system.Spark

/** Wrapper for a Job in Spark.
  *
  * The SparkJob follows the same procedure as launching a Spark application on the command line and uses
  * `./bin/spark-submit` internally. More information on the available command-options can be found at
  * [[https://spark.apache.org/docs/latest/submitting-applications.html]]
  *
  * @example Usage in a Spring XML context:
  *
  *  {{{
  *  <bean id="IdForMyBean" class="org.peelframework.spark.beans.job.SparkJob">
  *      <constructor-arg name="runner" ref="spark-1.1.0"/>
  *      <constructor-arg name="command">
  *          <value>
  *            --class org.peelframework.MyDatagenJob                                                       \
  *            \${app.path.someConfigVar}/my-datagen-job.jar                                                \
  *            \${system.spark.config.defaults.spark.master} \${system.default.config.parallelism.total} 100 \
  *            \${app.path.config}/inputfile.csv
  *          </value>
  *      </constructor-arg>
  *  </bean>
  *  }}}
  *
  * @note The master is set through the spark-config file and does not have to be added through the command.
  *
  * @param command Command to run the job (as specified by the specific system)
  * @param runner System that this job is written for
  * @param timeout Timeout limit for the job. If the job does not finish within the given limit, it is canceled.
  *                Defaults to 600 seconds.
 */
class SparkJob(command: String, runner: Spark, timeout: Long) extends Job(command, runner, timeout) {

  def this(command: String, runner: Spark) = this(command, runner, 600)

  def runJob() = {
    ensureFolderIsWritable(Paths.get(s"$home"))
    this ! resolve(command)
  }

  def cancelJob() = {}

  private def !(command: String) = {
    val master = config.getString(s"system.${runner.configKey}.config.defaults.spark.master")
    shell ! s"${config.getString(s"system.${runner.configKey}.path.home")}/bin/spark-submit --master $master ${command.trim} >> $home/run.out 2>> $home/run.err"
  }
}
