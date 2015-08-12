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
package org.peelframework.core.beans.data

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths}

import org.peelframework.core.beans.system.{FileSystem, System}

/** Dataset that is copied from a local filesystem to a specified target location.
  *
  * If the data already exists at the specified location, it is '''not''' copied again!
  *
  * @param src Local path where the data is stored.
  * @param dst Path in the distributed filesystem where the data is stored.
  * @param fs The filesystem that is used.
  */
class CopiedDataSet(val src: String, val dst: String, val fs: System with FileSystem) extends DataSet(dst, Set[System](fs)) {

  import scala.language.implicitConversions

  override def materialize() = {
    // resolve parameters from the current config in src and dst
    val dst = resolve(this.dst)
    val src = resolve(this.src)

    if (!Files.isRegularFile(Paths.get(src))) {
      throw new FileNotFoundException(s"Local static file at location '$src' does not exist!")
    }

    logger.info(s"Copying data set '$src' to '$dst'")
    if (fs.copyFromLocal(src, dst) != 0) throw new RuntimeException(s"Could not copy '$src' to '$dst'")
  }
}
