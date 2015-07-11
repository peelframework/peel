package eu.stratosphere.peel.core.cli

import java.net.InetAddress
import java.nio.file.Paths
import java.util.ServiceLoader

import eu.stratosphere.peel.core.cli.command.Command
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.ArgumentParserException
import net.sourceforge.argparse4j.internal.HelpScreenException
import org.apache.log4j.{PatternLayout, RollingFileAppender}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.collection.JavaConversions._

object Peel {

  def main(_args: Array[String]): Unit = {

    // empty args calls help
    val args = if (_args.length == 0) Array[String]("--help") else _args

    // load available commands
    val commands = Map((for (c <- ServiceLoader.load(classOf[Command]).iterator().toSeq) yield c.name -> c): _*)

    // construct base argument parser
    val parser = argumentParser

    // format command arguments with the arguments parser (in order of command names)
    for (key <- commands.keySet.toSeq.sorted) {
      val c = commands(key)
      c.register(parser.addSubparsers().addParser(c.name, true).help(c.help))
    }

    try {
      // parse the arguments
      val ns = parser.parseArgs(args)

      // format general options as system properties
      System.setProperty("app.command", ns.getString("app.command"))
      System.setProperty("app.hostname", ns.getString("app.hostname"))
      System.setProperty("app.path.config", Paths.get(ns.getString("app.path.config")).normalize().toAbsolutePath.toString)
      System.setProperty("app.path.log", Paths.get(ns.getString("app.path.log")).normalize().toAbsolutePath.toString)

      // add new root file appender
      val appender = new RollingFileAppender
      appender.setLayout(new PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n"))
      appender.setFile(String.format("%s/peel.log", System.getProperty("app.path.log")), true, true, 4096)
      appender.setMaxFileSize("100KB")
      appender.setMaxBackupIndex(1)
      org.apache.log4j.Logger.getRootLogger.addAppender(appender)

      // make sure that command exists
      if (!commands.containsKey(System.getProperty("app.command"))) {
        throw new RuntimeException(String.format("Unexpected command '%s'", System.getProperty("app.command")))
      }

      // execute command workflow:
      // 1) print application header
      val logger = LoggerFactory.getLogger(Peel.getClass)
      printHeader(logger)

      // 2) construct and configure command
      val command = commands(System.getProperty("app.command"))
      command.configure(ns)

      // 3) construct application context
      val context = new ClassPathXmlApplicationContext(
        if (null != System.getProperty("app.path.experiments"))
          Array[String]("classpath:peel-core.xml", "classpath:peel-extensions.xml", "file:" + System.getProperty("app.path.experiments"))
        else
          Array[String]("classpath:peel-core.xml", "classpath:peel-extensions.xml"),
        true)
      context.registerShutdownHook()

      // 4) execute command and return
      command.run(context);
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

  def argumentParser = {
    val parser = ArgumentParsers.newArgumentParser("peel")
      .defaultHelp(true)
      .description("A framework for execution of system experiments.")
    parser.addSubparsers()
      .help("a command to run")
      .dest("app.command")
      .metavar("COMMAND")

    // general options
    parser.addArgument("--hostname")
      .`type`(classOf[String])
      .dest("app.hostname")
      .metavar("NAME")
      .help("hostname for config resolution")
    parser.addArgument("--config")
      .`type`(classOf[String])
      .dest("app.path.config")
      .metavar("PATH")
      .help("config folder")
    parser.addArgument("--log")
      .`type`(classOf[String])
      .dest("app.path.log")
      .metavar("PATH")
      .help("log folder")

    // general option defaults
    parser.setDefault("app.hostname", hostname)
    parser.setDefault("app.path.config", "config")
    parser.setDefault("app.path.log", "log")

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
