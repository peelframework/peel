package eu.stratosphere.fab.core.beans

import org.slf4j.LoggerFactory
import scala.sys.process.{ProcessLogger, Process}

/**
 * Created by felix on 15.06.14.
 */
object Shell {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def execute(str: String, logOutput: Boolean) {
    val out = new StringBuilder
    val err = new StringBuilder
    // Use ProcessLogger to catch the results of stdout and strerr
    // Use bash to enable the use of bash features (e.g. wildcards)
    val exitcode = Process("/bin/bash", Seq("-c", str)) ! ProcessLogger(
      (s) => out.append(s+"\n"),
      (s) => err.append(s+"\n"));
    if (logOutput) {
      if (!out.toString.trim.isEmpty) {
        logger.info(" - result stdout: " + out) }
      if (!err.toString.trim.isEmpty) {
        logger.info(" - result strerr: " + err) }
    }
    (out.toString, err.toString, exitcode)
  }

  def rmDir(path: String) =
    logger.info(execute("rm -r " + path, true).toString)

  def untar(src: String, target: String) =
    logger.info(execute("tar -vxzf %s -C %s".format(src, target), true).toString)
}
