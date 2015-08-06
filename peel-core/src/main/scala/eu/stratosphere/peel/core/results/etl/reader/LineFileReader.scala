package eu.stratosphere.peel.core.results.etl.reader

import java.io.{BufferedReader, File}

/** A [[eu.stratosphere.peel.core.results.etl.reader.FileReader FileReader]] that creates a string for each line. */
case class LineFileReader(override val file: File) extends FileReader[Line] {

  /** Read the next [[eu.stratosphere.peel.core.results.etl.reader.Line Line]] from the given `reader`. */
  override def readNext(reader: BufferedReader) = Line(reader.readLine())
}

/** Entry type for the [[eu.stratosphere.peel.core.results.etl.reader.LineFileReader LineFileReader]]. */
case class Line(
  str: String
)
