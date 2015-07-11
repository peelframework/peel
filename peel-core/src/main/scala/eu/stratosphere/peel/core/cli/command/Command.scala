package eu.stratosphere.peel.core.cli.command

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
