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
