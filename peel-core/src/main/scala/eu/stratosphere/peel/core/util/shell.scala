package eu.stratosphere.peel.core.util

import java.io._
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.security.{DigestInputStream, DigestOutputStream, MessageDigest}

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
    val exit = Process("/bin/bash", Seq("-c", s"CLASSPATH=;$cmd")) ! plog
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
    val exit = Process("/bin/bash", Seq("-c", s"CLASSPATH=;$cmd")) !! plog
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

  /** Downloads a file from the given `src` URL to the `dst` path.
    *
    * @throws RuntimeException If the actual and the expected md5 hashes do not coincide.
    */
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

  /** Returns a ProcessLogger that is used to log shell output and error streams
    *
    * @return ProcessLogger for the executed command
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

  /** write std out to fout */
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
