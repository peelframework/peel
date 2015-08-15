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
package org.peelframework.core.results.etl.reader

import java.io.{BufferedReader, File}

import spray.json._

/** A [[FileReader]] that creates a JSON object for each line. */
case class JsonFileReader(override val file: File) extends FileReader[JsValue] {

  /** Read the next `spray.json.JsObject` from the given `reader`. */
  override def readNext(reader: BufferedReader): JsObject = {
    reader.readLine().parseJson.asJsObject
  }
}
