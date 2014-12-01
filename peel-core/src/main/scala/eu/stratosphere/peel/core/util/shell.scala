package eu.stratosphere.peel.core.util

import java.io._

import org.slf4j.LoggerFactory

import scala.sys.process.{Process, ProcessLogger}

/** Provides shell functionalities
  *
  * Wraps some commands of a unix bash-shell that are needed to execute commands like copying,
  * unzipping etc.
  */
object shell {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Executes a command in the bash shell
   *
   * @param cmd the command to execute
   * @return exit code of the command
   */
  def !(cmd: String) = {
    val plog = processLogger()
    plog.out("-" * 60)
    plog.out(cmd)
    plog.err("-" * 60)
    plog.err(cmd)
    val exit = Process("/bin/bash", Seq("-c", cmd)) ! plog
    plog.flush()
    plog.close()
    exit
  }

  /** Executes a command in the bash shell
   *
   * @param cmd the command to execute
   * @return result of the command as a string
   */
  def !!(cmd: String) = {
    val plog = processLogger()
    val exit = Process("/bin/bash", Seq("-c", cmd)) !! plog
    plog.flush()
    plog.close()
    exit
  }

  /** Removes a directory
    *
    * wraps /bin/bash rm -r
   *
   * @param path the directory to remove
   * @return 0 if successful, != else
   */
  def rmDir(path: String) = this ! s"rm -r $path"

  /** Extracts an archive
   *
    * wraps /bin/bash tar -xzf $src -C $dst
    *
   * @param src archive source path
   * @param dst target destination path
    * @throws RuntimeException if extraction was not successful
   */
  def extract(src: String, dst: String) = {
    if (this ! s"tar -xzf $src -C $dst" != 0) throw new RuntimeException(s"Could not extract '$src' to '$dst'")
  }

  /** Returns a processlogger that is used to log shell output and error streams
   *
   * @return OutputStreamProcesslogger for the executed command
   */
  private def processLogger() = new OutputStreamProcessLogger(
    new File("%s/shell.out".format(System.getProperty("app.path.log", "/tmp"))),
    new File("%s/shell.err".format(System.getProperty("app.path.log", "/tmp"))))
}

/** Logs the output and error streams and writes them to the specified files
 *
 * @param fout File to write stdout log to
 * @param ferr File tp write stderr log to
 */
private class OutputStreamProcessLogger(fout: File, ferr: File) extends ProcessLogger with Closeable with Flushable {

  private val o = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fout, true))))
  private val e = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ferr, true))))

  /** write std out to fout*/
  def out(s: => String) =
    o println s

  /** write std err to ferr */
  def err(s: => String) =
    e println s

  /** buffer function */
  def buffer[T](f: => T): T = f

  /** close writers */
  def close() = {
    o.close()
    e.close()
  }

  /** flush writers */
  def flush() = {
    o.flush()
    e.flush()
  }
}
