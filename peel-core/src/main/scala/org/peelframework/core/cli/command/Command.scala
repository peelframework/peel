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
package org.peelframework.core.cli.command

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext

abstract class Command {

  val logger = LoggerFactory.getLogger(this.getClass)

  /** The unique name for this command. */
  val name: String

  /** A one-sentence help for this command. */
  val help: String

  /** Registers the arguments and options to the CLI sub-parser for this command.
    *
    * @param parser The parent parser.
    */
  def register(parser: Subparser): Unit

  /** Configures the selected command using the parsed CLI arguments.
    *
    * @param ns The parsed CLI arguments namespace.
    */
  def configure(ns: Namespace): Unit

  /** Runs the command.
    *
    * @param context The application context.
    */
  def run(context: ApplicationContext)
}
