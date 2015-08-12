/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
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
package org.peelframework.core.results.etl

import spray.json.{NullOptions, DefaultJsonProtocol}

/** A case class representing the common fields ensured to be appearing in all `state.json` files.
  *
  * @param name The name of the experiment run.
  * @param suiteName The name of the enclosing suite.
  * @param command The name of the command.
  * @param runnerID The `ID` of the runner system.
  * @param runnerName The `name` of the runner system.
  * @param runnerVersion The `version` of the runner system.
  * @param runExitCode The exit code of this run (0 indicates success).
  * @param runTime The runtime (in milliseconds) of this run.
  */
case class RunState(
  name: String,
  suiteName: String,
  command: String,
  runnerID: String,
  runnerName: String,
  runnerVersion: String,
  runExitCode: Option[Int] = None,
  runTime: Long = 0) {}

/** An object for [[org.peelframework.core.results.etl.RunState RunState]] <-> JSON format conversion.
  *
  */
object RunStateProtocol extends DefaultJsonProtocol with NullOptions {
  implicit val stateFormat = jsonFormat8(RunState)
}
