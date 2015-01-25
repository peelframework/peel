package eu.stratosphere.peel.analyser.cli

import java.io.{FileNotFoundException, File}
import java.nio.file.Paths

import eu.stratosphere.peel.analyser.controller.ReportManager
import eu.stratosphere.peel.core.cli.command.Command
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.springframework.context.ApplicationContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
/**
 * Created by Fabian on 25.01.2015.
 */
class ShowReport extends Command{

  val LOGGER = LoggerFactory.getLogger(classOf[ShowReport])

  override def name(): String = "showReport"

  override def help(): String = "show a JasperReports Report"

  override def configure(ns: Namespace): Unit = {
    System.setProperty("app.peelanalyser.path", Paths.get(ns.getString("app.peelanalyser.path")).normalize().toAbsolutePath.toString)
  }

  override def register(parser: Subparser): Unit = {
    parser.addArgument("--path")
      .`type`(classOf[String])
      .dest("app.peelanalyser.path")
      .help("the patch to the Report you want to show")
  }

  override def run(context: ApplicationContext): Unit = {
    val path = System.getProperty("app.peelanalyser.path")

    try {
      val reportFile = new File(path)
      ReportManager.showReport(reportFile)
    } catch {
      case e: FileNotFoundException => LOGGER.error("Could not find the Report with path: " + path, e)
    }
  }


}
