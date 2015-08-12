/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
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
