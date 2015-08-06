package eu.stratosphere.peel.core.results.etl.reader

import java.io.{BufferedReader, File}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import resource._

/** A base trait for all file readers. */
trait FileReader[+A] extends Traversable[A] {

  /** The file that is read. */
  val file: File

  /** Traverse loop. */
  override def foreach[U](f: A => U): Unit = {
    for (reader <- managed(Files.newBufferedReader(file.toPath, StandardCharsets.UTF_8))) {
      while (reader.ready()) f(readNext(reader))
    }
  }

  /** Read the next `A` object from the given `reader`. */
  def readNext(reader: BufferedReader): A
}
