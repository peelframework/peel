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
package org.peelframework.core.cli

import java.nio.file.Paths

import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.ArgumentParserException
import net.sourceforge.argparse4j.internal.HelpScreenException
import org.apache.log4j.{Level, PatternLayout, RollingFileAppender}
import org.peelframework.core.PeelApplicationContext
import org.peelframework.core.cli.command.Command
import org.peelframework.core.config.hostname
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

object Peel {

  val JAVA_VERSION = """(\d+)\.(\d+).*""".r

  def main(_args: Array[String]): Unit = {

    // ensure sufficient Java version
    val javaVersion = System.getProperty("java.version", "0.0") match {
      case JAVA_VERSION(major, minor) => major.toInt * 1000 + minor.toInt
      case _ => 0
    }

    if (javaVersion < 1008) {
      System.err.println(s"Peel requires Java 1.8+, but you are running ${System.getProperty("java.version")}.")
      System.err.println(s"Please point to a supported Java version in $$JAVA_HOME or directly in peel.sh.")
      System.exit(+1)
    }

    // empty args calls help
    val args = if (_args.length == 0) Array[String]("--help") else _args

    // construct argument parser for basic options
    var parser = argumentParser(basicOnly = true)

    // parse and store basic options
    try {
      // parse the arguments
      val ns = parser.parseArgs(args.takeWhile(arg => arg.startsWith("-") || arg.startsWith("--")))

      System.setProperty("app.hostname", ns.getString("app.hostname"))
      System.setProperty("app.log.level", ns.getString("app.log.level"))
      System.setProperty("app.path.config", Paths.get("config").normalize().toAbsolutePath.toString)
      System.setProperty("app.path.log", Paths.get("log").normalize().toAbsolutePath.toString)

    } catch {
      case e: Throwable =>
        System.err.println(String.format("Unexpected error while parsing basic arguments: %s", e.getMessage))
        System.exit(-1)
    }

    // add new root file appender
    val appender = new RollingFileAppender
    appender.setLayout(new PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n"))
    appender.setFile(String.format("%s/peel.log", System.getProperty("app.path.log", "log")), true, true, 4096)
    appender.setMaxFileSize("100KB")
    appender.setMaxBackupIndex(1)
    org.apache.log4j.Logger.getRootLogger.addAppender(appender)
    org.apache.log4j.Logger.getRootLogger.setLevel(Level.toLevel(System.getProperty("app.log.level")))

    // construct application context
    val context = PeelApplicationContext(Option(System.getProperty("app.path.config")))

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
      .dest("app.hostname")
      .metavar("NAME")
      .help(s"environment hostname (default: $hostname)")
    parser.addArgument("--loglevel")
      .`type`(classOf[String])
      .dest("app.log.level")
      .choices("OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL")
      .metavar("LEVEL")
      .help(s"log4j log level (default: INFO)")

    if (!basicOnly) {
      parser.addSubparsers()
        .help("a command to run")
        .dest("app.command")
        .metavar("COMMAND")
    } else {
      parser.addArgument("--help", "-h")
        .`type`(classOf[Boolean])
        .dest("app.printHelp")
        .action(Arguments.storeTrue)
        .help("show this help message and exit")
    }

    // general option defaults
    parser.setDefault("app.hostname", hostname)
    parser.setDefault("app.log.level", "INFO")

    parser
  }

  def printHeader(logger: Logger) {
    logger.info("############################################################")
    logger.info("#           PEEL EXPERIMENTS EXECUTION FRAMEWORK           #")
    logger.info("############################################################")
  }
}
