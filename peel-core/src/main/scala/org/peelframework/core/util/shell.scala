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
package org.peelframework.core.util

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

import org.peelframework.core.util.console._
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.{GzipCompressorInputStream, GzipCompressorOutputStream}
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

  /** Executes a command in the bash shell.
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

  /** Executes a command in the bash shell.
    *
    * @param cmd the command to execute
    * @return exit code of the command
    */
  def !(cmd: String, errorMsg: String, fatal: Boolean = true): Int = {
    val exit = this ! cmd.trim

    if (exit != 0) {
      if (fatal) throw new RuntimeException(errorMsg + "\nThe command that failed:\n" + cmd + "\n")
      else logger.error(errorMsg.red)
    }
    exit
  }

  /** Executes a command in the bash shell.
    *
    * @param cmd the command to execute
    * @return result of the command as a string
    */
  def !!(cmd: String) = {
    val plog = processLogger()
    plog.in(cmd)
    val exit = Process("/bin/bash", Seq("-c", s"CLASSPATH=;${cmd.trim}")) !! plog
    plog.flush()
    plog.close()
    exit
  }

  /** Checks if the given path is a writable folder.
    *
    * If the folder at the given path does not exists, it is created.
    * If it exists but is not a directory or is not writable, this method throws
    * a RuntimeException.
    *
    * @param folder path to the folder
    * @return Unit
    * @throws RuntimeException if folder exists but is not a writable directory
    */
  final def ensureFolderIsWritable(folder: Path): Unit = {
    if (Files.exists(folder)) {
      if (!(Files.isDirectory(folder) && Files.isWritable(folder))) {
        throw new RuntimeException(s"Folder '$folder' is not a writable directory")
      }
    } else {
      Files.createDirectories(folder)
    }
  }

  /** Touches a file.
    *
    * @param path the file to touch
    */
  def touch(path: String) = FileUtils.touch(new File(path))

  /** Removes a file.
    *
    * @param path the file to remove
    */
  def rm(path: String) = FileUtils.forceDelete(new File(path))

  /** Removes a directory recursively.
    *
    * @param path the directory to remove
    */
  def rmDir(path: String) = FileUtils.deleteDirectory(new File(path))

  /** List all directory structure descendants for a given root.
    *
    * @param root The root File.
    * @return A stream of File entries located under the given `root`.
    *
    * @see http://stackoverflow.com/questions/2637643/how-do-i-list-all-files-in-a-subdirectory-in-scala
    */
  def fileTree(root: File): Stream[File] = {
    root #:: (if (root.isDirectory) root.listFiles().toStream.flatMap(fileTree) else Stream.empty)
  }

  /** Extracts an archive.
    *
    * @param src Archive source path.
    * @param dst Extraction destination path.
    * @throws RuntimeException If extraction was not successful.
    * @throws IllegalArgumentException If the `src` does not end on supported archive suffix.
    */
  def extract(src: String, dst: String) = {
    import resource._

    def decompress(tar: TarArchiveInputStream): Unit = for (inp <- managed(Channels.newChannel(tar))) {
      implicit val buffer = ByteBuffer.allocate(1024 * 1024) // allocate 1 MB copy buffer

      var entry = tar.getNextEntry.asInstanceOf[TarArchiveEntry] // initialize current entry
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
            fil <- Some(new File(dst, entry.getName)) // construct the file name
            pat <- Some(Files.createDirectories(fil.toPath.getParent)) // create and construct the parent folder
            out <- managed(Channels.newChannel(new FileOutputStream(fil))) // entry output channel
            res <- Some(copy(inp, out)) // materialize entry
          } {
            Files.setPosixFilePermissions(outputPath, entry.getMode) // fix entry permissions
          }

        }

        entry = tar.getNextEntry.asInstanceOf[TarArchiveEntry] // advance entry
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
        inp <- managed(new BufferedInputStream(new FileInputStream(src)))
        tar <- managed(new TarArchiveInputStream(new GzipCompressorInputStream(inp)))
      } decompress(tar)
    } else {
      throw new IllegalStateException(s"Unsupported archive suffix for input '$src'")
    }
  }

  /** Compresses a folder into an archive.
    *
    * @param src Compression source path.
    * @param dst Archive destination path.
    * @throws RuntimeException if compression was not successful
    * @throws IllegalArgumentException If the `dst` does not end on supported archive suffix.
    */
  def archive(src: String, dst: String) = {
    import resource._

    val srcPath = Paths.get(src).getParent

    def compress(tar: TarArchiveOutputStream): Unit = for (out <- managed(Channels.newChannel(tar))) {
      implicit val buffer = ByteBuffer.allocate(1024 * 1024) // allocate 1 MB copy buffer

      tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU) // enable long entry names

      for {
        fle <- fileTree(new File(src)) // iterate over entry files
        rel <- Some(srcPath.relativize(fle.toPath)) // entry relative path
        ent <- Some(tar.createArchiveEntry(fle, rel.toString).asInstanceOf[TarArchiveEntry]) // archive entry
      } {
        ent.setMode(Files.getPosixFilePermissions(fle.toPath)) // se proper permissions
        tar.putArchiveEntry(ent) // open entry
        if (!fle.isDirectory) /* regular file */ {
          for (inp <- managed(Channels.newChannel(new FileInputStream(fle)))) /* entry output channel */ {
            copy(inp, out) // copy file contents to entry
          }
        }
        tar.closeArchiveEntry() // close entry
      }
    }

    // supported suffixes for gzipped tars
    if (List("tar.gz", "tgz").exists(suffix => dst.endsWith(suffix))) {
      for {
        out <- managed(new BufferedOutputStream(new FileOutputStream(dst)))
        tar <- managed(new TarArchiveOutputStream(new GzipCompressorOutputStream(out)))
      } compress(tar)
    } else {
      throw new IllegalStateException(s"Unsupported archive suffix for input '$src'")
    }
  }

  /** Downloads a file located at the given `url` at the specified `dst` and validates it against an MD5 sum.
    *
    * @param url The source URL.
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
    val inp = Paths.get("%s/shell.in".format(System.getProperty("app.path.log", "/tmp")))
    val out = Paths.get("%s/shell.out".format(System.getProperty("app.path.log", "/tmp")))
    val err = Paths.get("%s/shell.err".format(System.getProperty("app.path.log", "/tmp")))
    if (withTimeStamps)
      new OutputStreamProcessLogger(inp, out, err) with TimeStamps
    else
      new OutputStreamProcessLogger(inp, out, err)
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
    * @param mode An encoded permissions bitstring.
    * @return The corresponding [[java.nio.file.attribute.PosixFilePermission PosixFilePermission]] set.
    */
  implicit private def convIntToPermissionsSet(mode: Int): java.util.Set[PosixFilePermission] = {
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

  /** Converts the [[java.nio.file.attribute.PosixFilePermission PosixFilePermission]] set to an integer.
    *
    * @param perm A [[java.nio.file.attribute.PosixFilePermission PosixFilePermission]] set.
    * @return The corresponding encoded permissions bitstring.
    */
  implicit private def convPermissionsSetToInt(perm: java.util.Set[PosixFilePermission]): Int = {
    var result: Int = 0
    if (perm.contains(PosixFilePermission.OWNER_READ)) /*    */ result = result | (1 << 8)
    if (perm.contains(PosixFilePermission.OWNER_WRITE)) /*   */ result = result | (1 << 7)
    if (perm.contains(PosixFilePermission.OWNER_EXECUTE)) /* */ result = result | (1 << 6)
    if (perm.contains(PosixFilePermission.GROUP_READ)) /*    */ result = result | (1 << 5)
    if (perm.contains(PosixFilePermission.GROUP_WRITE)) /*   */ result = result | (1 << 4)
    if (perm.contains(PosixFilePermission.GROUP_EXECUTE)) /* */ result = result | (1 << 3)
    if (perm.contains(PosixFilePermission.OTHERS_READ)) /*   */ result = result | (1 << 2)
    if (perm.contains(PosixFilePermission.OTHERS_WRITE)) /*  */ result = result | (1 << 1)
    if (perm.contains(PosixFilePermission.OTHERS_EXECUTE)) /**/ result = result | (1 << 0)
    result
  }
}


/** Logs the output and error streams and writes them to the specified files
  *
  * @param fi File to write executed command to
  * @param fo File to write stdout log to
  * @param fe File to write stderr log to
  */
private class OutputStreamProcessLogger(fi: Path, fo: Path, fe: Path) extends ProcessLogger with Closeable with Flushable {

  val i = new PrintWriter(Files.newBufferedWriter(fi, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND), true)
  val o = new PrintWriter(Files.newBufferedWriter(fo, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND), true)
  val e = new PrintWriter(Files.newBufferedWriter(fe, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND), true)

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
