package eu.stratosphere.peel.core.util

import java.io._

import org.slf4j.LoggerFactory

import scala.sys.process.{Process, ProcessLogger}

object shell {

  private val logger = LoggerFactory.getLogger(this.getClass)

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


  def !!(cmd: String) = {
    val plog = processLogger()
    val exit = Process("/bin/bash", Seq("-c", cmd)) !! plog
    plog.flush()
    plog.close()
    exit
  }

  def rmDir(path: String) = this ! s"rm -r $path"

  def untar(src: String, dst: String) = if (this ! s"tar -xzf $src -C $dst" != 0) throw new RuntimeException(s"Could not extract '$src' to '$dst'")

  private def processLogger() = new OutputStreamProcessLogger(
    new File("%s/shell.out".format(System.getProperty("app.path.log", "/tmp"))),
    new File("%s/shell.err".format(System.getProperty("app.path.log", "/tmp"))))
}

private class OutputStreamProcessLogger(fout: File, ferr: File) extends ProcessLogger with Closeable with Flushable {

  private val o = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fout, true))))
  private val e = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(ferr, true))))

  def out(s: => String) =
    o println s

  def err(s: => String) =
    e println s

  def buffer[T](f: => T): T = f

  def close() = {
    o.close()
    e.close()
  }

  def flush() = {
    o.flush()
    e.flush()
  }
}
