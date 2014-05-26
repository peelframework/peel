/**
 * Created by felix on 27.04.14.
 */
package extensions

import scala.sys.process._
import scala.collection.mutable.StringBuilder
import common.Shell
import org.slf4j.LoggerFactory
import org.apache.log4j.PropertyConfigurator

object BashShell extends Shell{
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
}
