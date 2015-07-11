package eu.stratosphere.peel.core.util

import java.io._
import java.net.URL
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.charset.StandardCharsets
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.security.{DigestInputStream, DigestOutputStream, MessageDigest}
import java.text.SimpleDateFormat
import java.util.Date

import eu.stratosphere.peel.core.util.console._
import org.apache.commons.compress.archivers.ArchiveInputStream
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import resource._

import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.sys.process.{Process, ProcessLogger}

/** Provides shell functionality.
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
  def rmDir(path: String) = FileUtils.deleteDirectory(new File(path))

  /** Extracts an archive.
    *
    * @param src archive source path
    * @param dst target destination path
    * @throws RuntimeException if extraction was not successful
    * @throws IllegalArgumentException If the src does not end on supported archive suffix.
    */
  def extract(src: String, dst: String) = {
    import resource._

    def decompress[T <: TarArchiveEntry](ar: ArchiveInputStream): Unit = for (inp <- managed(Channels.newChannel(ar))) {
      implicit val buffer = ByteBuffer.allocate(1024 * 1024) // allocate 1 MB copy buffer

      var entry = ar.getNextEntry.asInstanceOf[T] // initialize current entry
      var links = Map.newBuilder[Path, Path] // initialize links accumulator map

      // traverse archive entries
      while (entry != null) {
        // get entry file
        val outputFile = new File(dst, entry.getName)
        val outputPath = Paths.get(outputFile.getPath)

        // handle entry
        if (entry.getLinkName.nonEmpty && !outputFile.exists()) /* link entry */ {
          links += outputPath -> Paths.get(entry.getLinkName)

        } else if (entry.isDirectory) /* directory */ {
          if (!outputFile.exists() && !outputFile.mkdirs()) {
            throw new RuntimeException(s"Could not create dir $outputFile")
          }

          Files.setPosixFilePermissions(outputPath, entry.getMode)

        } else /* regular file */ {
          for {
            out <- managed(Channels.newChannel(new FileOutputStream(new File(dst, entry.getName)))) // entry output channel
            res <- Some(copy(inp, out)) // materialize entry
          } {
            Files.setPosixFilePermissions(outputPath, entry.getMode) // fix entry permissions
          }

        }

        // advance entry
        entry = ar.getNextEntry.asInstanceOf[T]
      }

      // create symbolic links
      for {
        lnm <- Some(links.result()) // construct 'link -> target' paths map
        lnk <- lnm.keys // link path
        tgt <- lnm.get(lnk) // target path
      } {
        Files.createSymbolicLink(lnk, tgt)
      }
    }

    // supported suffixes for gzipped tars
    if (List("tar.gz", "tgz").exists(suffix => src.endsWith(suffix))) {
      for {
        in <- managed(new BufferedInputStream(new FileInputStream(src)))
        ar <- managed(new TarArchiveInputStream(new GzipCompressorInputStream(in)))
      } decompress[TarArchiveEntry](ar)
    } else {
      throw new IllegalStateException(s"Unsupported archive suffix for input '$src'")
    }
  }

  /** Downloads a file located at the given `url` at the specified `dst` and validates it against an MD5 sum.
    *
    * @param url The source [[java.net.URL URL]].
    * @param dst The target destination.
    * @param exp The expected MD5 sum.
    */
  def download(url: String, dst: String, exp: BigInt) = {
    implicit val buffer = ByteBuffer.allocate(1024 * 1024) // allocate 1 MB copy buffer
    val md5 = MessageDigest.getInstance("MD5") // allocate md5 digest

    for {
      inp <- managed(Channels.newChannel(new URL(url).openStream())) // input channel
      out <- managed(Channels.newChannel(new DigestOutputStream(new FileOutputStream(dst), md5))) // output channel
      res <- Some(copy(inp, out)) // copy contents
      act <- Some(BigInt(1, md5.digest())) // compute MD5
    } {
      assert(act == exp, s"MD5 mismatch for file '$dst': expected ${exp.toString(16)}, got ${act.toString(16)}") // check MD5
    }
  }

  /** Check the `md5` hash of the file at the given `path` against an expected value.
    *
    * @throws RuntimeException If the actual and the expected md5 hashes do not coincide.
    */
  def checkMD5(path: String, exp: BigInt) = {
    implicit val buffer = ByteBuffer.allocate(1024 * 1024) // allocate 1 MB copy buffer
    val md5 = MessageDigest.getInstance("MD5") // allocate md5 digest

    for {
      inp <- managed(Channels.newChannel(new DigestInputStream(new FileInputStream(path), md5))) // input channel
      res <- Some(while (inp.read(buffer) != -1) buffer.clear()) // consume channel
      act <- Some(BigInt(1, md5.digest())) // compute MD5
    } {
      assert(act == exp, s"MD5 mismatch for file '$path': expected ${exp.toString(16)}, got ${act.toString(16)}") // check MD5
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

  /** Copy the contents of a [[java.nio.channels.ReadableByteChannel ReadableByteChannel]] to a
    * [[java.nio.channels.WritableByteChannel WritableByteChannel]].
    *
    * @param inp The input channel.
    * @param out The output channel.
    * @param buffer A buffer to be used for copying.
    */
  @tailrec
  private def copy(inp: ReadableByteChannel, out: WritableByteChannel)(implicit buffer: ByteBuffer): Unit = inp.read(buffer) match {
    case -1 => ()
    case n => buffer.flip(); out.write(buffer); buffer.clear(); copy(inp, out)
  }

  /** Converts the 9 least significant bits of an integer to a [[java.nio.file.attribute.PosixFilePermission PosixFilePermission]] set.
    *
    * @param mode The encoded permission string
    * @return The corresponding [[java.nio.file.attribute.PosixFilePermission PosixFilePermission]] set.
    */
  implicit private def convertToPermissionsSet(mode: Int): java.util.Set[PosixFilePermission] = {
    val result = java.util.EnumSet.noneOf(classOf[PosixFilePermission])
    if ((mode & (1 << 8)) != 0) result.add(PosixFilePermission.OWNER_READ)
    if ((mode & (1 << 7)) != 0) result.add(PosixFilePermission.OWNER_WRITE)
    if ((mode & (1 << 6)) != 0) result.add(PosixFilePermission.OWNER_EXECUTE)
    if ((mode & (1 << 5)) != 0) result.add(PosixFilePermission.GROUP_READ)
    if ((mode & (1 << 4)) != 0) result.add(PosixFilePermission.GROUP_WRITE)
    if ((mode & (1 << 3)) != 0) result.add(PosixFilePermission.GROUP_EXECUTE)
    if ((mode & (1 << 2)) != 0) result.add(PosixFilePermission.OTHERS_READ)
    if ((mode & (1 << 1)) != 0) result.add(PosixFilePermission.OTHERS_WRITE)
    if ((mode & (1 << 0)) != 0) result.add(PosixFilePermission.OTHERS_EXECUTE)
    result
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
