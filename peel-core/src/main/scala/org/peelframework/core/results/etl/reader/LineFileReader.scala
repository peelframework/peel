package org.peelframework.core.results.etl.reader

import java.io.{BufferedReader, File}

/** A [[org.peelframework.core.results.etl.reader.FileReader FileReader]] that creates a string for each line. */
case class LineFileReader(override val file: File) extends FileReader[Line] {

  /** Read the next [[org.peelframework.core.results.etl.reader.Line Line]] from the given `reader`. */
  override def readNext(reader: BufferedReader) = Line(reader.readLine())
}

/** Entry type for the [[org.peelframework.core.results.etl.reader.LineFileReader LineFileReader]]. */
case class Line(
  str: String
)
