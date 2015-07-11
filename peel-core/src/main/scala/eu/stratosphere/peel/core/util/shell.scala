package eu.stratosphere.peel.core.util

import java.io._
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, StandardOpenOption, Files}
import java.security.{DigestInputStream, DigestOutputStream, MessageDigest}
import java.text.SimpleDateFormat
import java.util.Date

import org.slf4j.LoggerFactory

import eu.stratosphere.peel.core.util.console._

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
  def !(cmd: String): Int = {
    val plog = processLogger()
    plog.in(cmd)
    val exit = Process("/bin/bash", Seq("-c", s"CLASSPATH=;${cmd.trim}")) ! plog
    plog.flush()
    plog.close()
    exit
  }

  /** Executes a command in the bash shell
    *
    * @param cmd the command to execute
    * @return exit code of the command
    */
  def !(cmd: String, errorMsg: String, fatal: Boolean = true): Int = {
    val exit = this ! cmd.trim

    if (exit != 0) {
      if (fatal) throw new RuntimeException(errorMsg)
      else logger.error(errorMsg.red)
    }
    exit
  }

  /** Executes a command in the bash shell
    *
    * @param cmd the command to execute
    * @return result of the command as a string
    */
  def !!(cmd: String) = {
    val plog = processLogger()
    val exit = Process("/bin/bash", Seq("-c", s"CLASSPATH=;${cmd.trim}")) !! plog
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
    this !(s"tar -xzf $src -C $dst", s"Could not extract '$src' to '$dst'")
  }

  def download(src: String, dst: String, md5Exp: BigInt) = {
    val md5Digest = MessageDigest.getInstance("MD5")
    val in = Channels.newChannel(new URL(src).openStream())
    val out = Channels.newChannel(new DigestOutputStream(new FileOutputStream(dst), md5Digest))

    val buffer = ByteBuffer.allocate(1024 * 1024) // 1 MB
    try {
      while (in.read(buffer) != -1) {
        buffer.flip()
        out.write(buffer)
        buffer.clear()
      }

      val md5Act = BigInt(1, md5Digest.digest())
      if (md5Act != md5Exp) {
        throw new RuntimeException(s"MD5 mismatch for file '$dst': expected ${md5Exp.toString(16)}, got ${md5Act.toString(16)}")
      }
    } finally {
      in.close()
      out.close()
    }
  }

  /** Check the `md5` hash of the file at the given `path` against an expected value.
    *
    * @throws RuntimeException If the actual and the expected md5 hashes do not coincide.
    */
  def checkMD5(path: String, md5Exp: BigInt) = {
    val md5Digest = MessageDigest.getInstance("MD5")
    val in = new DigestInputStream(new FileInputStream(path), md5Digest)

    val buffer = Array.fill[Byte](1024 * 1024)(0) // 1 MB
    try {
      while (in.read(buffer) != -1) {}

      val md5Act = BigInt(1, md5Digest.digest())
      if (md5Act != md5Exp) {
        throw new RuntimeException(s"MD5 mismatch for file '$path': expected ${md5Exp.toString(16)}, got ${md5Act.toString(16)}")
      }
    } finally {
      in.close()
    }
  }

  /** Returns a processlogger that is used to log shell output and error streams
    *
    * @param withTimeStamps adds timestamps to the output
    *
    * @return OutputStreamProcesslogger for the executed command
    */
  private def processLogger(withTimeStamps: Boolean = true) = {
    val in = Paths.get("%s/shell.in".format(System.getProperty("app.path.log", "/tmp")))
    val out = Paths.get("%s/shell.out".format(System.getProperty("app.path.log", "/tmp")))
    val err = Paths.get("%s/shell.err".format(System.getProperty("app.path.log", "/tmp")))
    if (withTimeStamps)
      new OutputStreamProcessLogger(in, out, err) with TimeStamps
    else
      new OutputStreamProcessLogger(in, out, err)
  }
}


/** Logs the output and error streams and writes them to the specified files
  *
  * @param fin File to write executed command to
  * @param fout File to write stdout log to
  * @param ferr File to write stderr log to
  */
private class OutputStreamProcessLogger(fin: Path, fout: Path, ferr: Path) extends ProcessLogger with Closeable with Flushable {

  val i = new PrintWriter(Files.newBufferedWriter(fin, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
  val o = new PrintWriter(Files.newBufferedWriter(fout, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
  val e = new PrintWriter(Files.newBufferedWriter(ferr, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND))

  /** write std in to fin */
  def in(s: => String) = i println s

  /** write std out to fout */
  override def out(s: => String) = o println s

  /** write std err to ferr */
  override def err(s: => String) = e println s

  /** buffer function */
  def buffer[T](f: => T): T = f

  /** close writers */
  def close() = {
    i.close()
    o.close()
    e.close()
  }

  /** flush writers */
  def flush() = {
    i.flush()
    o.flush()
    e.flush()
  }
}

private trait TimeStamps {
  logger: OutputStreamProcessLogger =>

  /* We only want to add the timestamp once per cmd, therefore the must keep track whether something was written
   * to the corresponding writer.
   */
  var iEmpty = true
  var oEmpty = true
  var eEmpty = true

  val TimeStamp = {
    val dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss.SSS")
    () => s"# ${dateFormat.format(new Date())}"
  }

  /** write std in to fin */
  override def in(s: => String) = {
    if (iEmpty && s.nonEmpty) {
      logger.i println TimeStamp()
      eEmpty = false
    }
    logger.i println s
  }

  /** write std in to fout */
  override def out(s: => String) = {
    if (oEmpty && s.nonEmpty) {
      logger.o println TimeStamp()
      eEmpty = false
    }
    logger.o println s
  }

  /** write std in to ferr */
  override def err(s: => String) = {
    if (eEmpty && s.nonEmpty) {
      logger.e println TimeStamp()
      eEmpty = false
    }
    logger.e println s
  }
}
