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
package org.peelframework.core.results.etl.traverser

import java.io.File
import java.nio.file.Path

import com.typesafe.config.Config
import org.peelframework.core.util.shell.fileTree

/** Traverse the file entries for a state run.
  *
  * @param runPath The path to be traversed.
  * @param config The application configuration.
  */
class RunTraverser(runPath: Path)(implicit config: Config) extends Traversable[File] {

  override def foreach[U](f: File => U): Unit = for {
    file <- fileTree(runPath.toFile) if file.isFile && file.canRead
  } f(file)
}

/** Companion object. */
object RunTraverser {

  def apply(runPath: Path)(implicit config: Config) = new RunTraverser(runPath)
}
