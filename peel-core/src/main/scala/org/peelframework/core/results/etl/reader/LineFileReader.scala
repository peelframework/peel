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

/** A [[org.peelframework.core.results.etl.reader.FileReader FileReader]] that creates a string for each line. */
case class LineFileReader(override val file: File) extends FileReader[Line] {

  /** Read the next [[org.peelframework.core.results.etl.reader.Line Line]] from the given `reader`. */
  override def readNext(reader: BufferedReader) = Line(reader.readLine())
}

/** Entry type for the [[org.peelframework.core.results.etl.reader.LineFileReader LineFileReader]]. */
case class Line(
  str: String
)
