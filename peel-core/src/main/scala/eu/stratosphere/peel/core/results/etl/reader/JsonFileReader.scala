package eu.stratosphere.peel.core.results.etl.reader

import java.io.{BufferedReader, File}

import spray.json._

/** A [[eu.stratosphere.peel.core.results.etl.reader.FileReader FileReader]] that creates a JSON object for each line. */
case class JsonFileReader(override val file: File) extends FileReader[JsValue] {

  /** Read the next [[spray.json.JsObject JsObject]] from the given `reader`. */
  override def readNext(reader: BufferedReader): JsObject = {
    reader.readLine().parseJson.asJsObject
  }
}
