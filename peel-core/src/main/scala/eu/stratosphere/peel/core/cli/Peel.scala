package eu.stratosphere.peel.core.cli

import java.net.InetAddress
import java.nio.file.Paths

import eu.stratosphere.peel.core.PeelApplicationContext
import eu.stratosphere.peel.core.cli.command.Command
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParserException
import net.sourceforge.argparse4j.internal.HelpScreenException
import org.apache.log4j.{PatternLayout, RollingFileAppender}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object Peel {

  def main(_args: Array[String]): Unit = {

    // empty args calls help
    val args = if (_args.length == 0) Array[String]("--help") else _args

    // construct argument parser for basic options
    var parser = argumentParser(basicOnly = true)

    // parse and store basic options
    try {
      // parse the arguments
      val ns = parser.parseArgs(args.takeWhile(arg => arg.startsWith("-") || arg.startsWith("--")))

      for (v <- Option(ns.getString("app.hostname"))) {
        System.setProperty("app.hostname", ns.getString("app.hostname"))
      }
      for (v <- Option(ns.getString("app.path.config"))) {
        System.setProperty("app.path.config", Paths.get(v).normalize().toAbsolutePath.toString)
      }
      for (v <- Option(ns.getString("app.path.log"))) {
        System.setProperty("app.path.log", Paths.get(v).normalize().toAbsolutePath.toString)
      }
      for (v <- Option(ns.getString("app.path.experiments"))) {
        System.setProperty("app.path.experiments", Paths.get(v).normalize.toAbsolutePath.toString)
      }
    } catch {
      case e: Throwable =>
        System.err.println(String.format("Unexpected error while parsing basic arguments: %s", e.getMessage))
        System.exit(-1)
    }

    // construct application context
    val context = PeelApplicationContext(Option(System.getProperty("app.path.experiments")))

    // construct argument parser with commands
    parser = argumentParser(basicOnly = false)

    // load available commands
    val commands = (for (key <- context.getBeanNamesForType(classOf[Command]).sorted) yield {
      val command = context.getBean(key, classOf[Command])
      command.register(parser.addSubparsers().addParser(command.name, true).help(command.help))
      key -> command
    }).toMap

    try {
      // parse the arguments
      val ns = parser.parseArgs(args)

      // add new root file appender
      val appender = new RollingFileAppender
      appender.setLayout(new PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n"))
      appender.setFile(String.format("%s/peel.log", System.getProperty("app.path.log")), true, true, 4096)
      appender.setMaxFileSize("100KB")
      appender.setMaxBackupIndex(1)
      org.apache.log4j.Logger.getRootLogger.addAppender(appender)

      Option(ns.getString("app.command")) match {
        case None =>
          throw new RuntimeException("Missing command name. Type '--help' to see the list of the available commands. ")
        case Some(commandName) if !commands.containsKey(commandName) =>
          throw new RuntimeException(s"Unexpected command '$commandName'")
        case Some(commandName) if commands.containsKey(commandName) =>
          // 1) create logger
          val logger = LoggerFactory.getLogger(Peel.getClass)
          // 2) print application header
          printHeader(logger)
          // 3) get command
          val command = commands(commandName)
          // 4) configure and run command (TODO: rewrite as single method)
          command.configure(ns)
          command.run(context);
      }
    } catch {
      case e: HelpScreenException =>
        parser.handleError(e)
        System.exit(0)
      case e: ArgumentParserException =>
        parser.handleError(e)
        System.exit(-1);
      case e: Throwable =>
        System.err.println(String.format("Unexpected error: %s", e.getMessage))
        e.printStackTrace()
        System.exit(-1);
    }
  }

  def argumentParser(basicOnly: Boolean = true) = {
    // construct parser
    val parser = ArgumentParsers.newArgumentParser("peel", !basicOnly) // do not add help in 'basicOnly' mode
      .description("A framework for execution of system experiments.")

    // general options
    parser.addArgument("--hostname")
      .`type`(classOf[String])
      .dest("app.hostname (app.hostname)")
      .metavar("NAME")
      .help("hostname for config resolution (app.hostname)")
    parser.addArgument("--config")
      .`type`(classOf[String])
      .dest("app.path.config")
      .metavar("PATH")
      .help("config folder (app.path.config)")
    parser.addArgument("--experiments")
      .`type`(classOf[String])
      .dest("app.path.experiments")
      .metavar("EXPFILE")
      .help("experiments spec (default: config/experiments.xml)")
    parser.addArgument("--log")
      .`type`(classOf[String])
      .dest("app.path.log")
      .metavar("PATH")
      .help("log folder (app.path.log)")

    if (!basicOnly) {
      parser.addSubparsers()
        .help("a command to run")
        .dest("app.command")
        .metavar("COMMAND")
    } else {
      parser.addArgument("--help", "-f")
        .`type`(classOf[Boolean])
        .dest("app.printHelp")
        .action(Arguments.storeTrue)
        .help("show this help message and exit")
    }

    // general option defaults
    parser.setDefault("app.hostname", hostname)
    parser.setDefault("app.path.config", "config")
    parser.setDefault("app.path.log", "log")
    parser.setDefault("app.path.experiments", "config/experiments.xml")

    parser
  }

  def hostname = {
    try {
      InetAddress.getLocalHost.getHostName
    } catch {
      case _: Throwable => "localhost"
    }
  }

  def printHeader(logger: Logger) {
    logger.info("############################################################")
    logger.info("#           PEEL EXPERIMENTS EXECUTION FRAMEWORK           #")
    logger.info("############################################################")
  }
}
