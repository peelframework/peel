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
package org.peelframework.hadoop.beans.job

import java.nio.file.Paths

import org.peelframework.core.beans.system.Job
import org.peelframework.core.util.shell
import org.peelframework.hadoop.beans.system.Yarn

/** Wrapper for a Job in Yarn.
 *
 * The YarnJob follows the same procedure as launching a Yarn application on the command line and uses
 * `./bin/yarn jar` internally. More information on the available command-options can be found at
 * [[https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/YarnCommands.html]]
 *
 * @example Usage in a Spring XML context:
 *
 *  {{{
 *  <bean id="IdForMyBean" class="org.peelframework.hadoop.beans.job.YarnJob">
 *      <constructor-arg name="runner" ref="yarn-2.7.1"/>
 *      <constructor-arg name="command">
 *          <value>
 *            {app.path.someConfigVar}/my-datagen-job.jar                                            \
 *            org.peelframework.MyDatagenJob                                                              \
 *            \${app.path.config}/inputfile.csv
 *          </value>
 *      </constructor-arg>
 *  </bean>
 *  }}}
 * @param runner System that this job is written for
 * @param timeout Timeout limit for the job. If the job does not finish within the given limit, it is canceled.
 *                Defaults to 600 seconds.
 */
class YarnJob(command: String, runner: Yarn, timeout: Long) extends Job(command, runner, timeout) {

  def this(command: String, runner: Yarn) = this(command, runner, 600)

  def runJob() = {
    ensureFolderIsWritable(Paths.get(s"$home"))
    this ! resolve(command)
  }

  def cancelJob() = {}

  private def !(command: String) = {
    shell ! s"${config.getString(s"system.${runner.configKey}.path.home")}/bin/yarn jar ${command.trim} >> $home/run.out 2>> $home/run.err"
  }
}
