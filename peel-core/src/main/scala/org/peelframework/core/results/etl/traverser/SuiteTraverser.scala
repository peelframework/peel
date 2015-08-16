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
import org.peelframework.core.results.etl.{GenericRunState, RunStateProtocol}
import resource._
import spray.json._

/** Traverse the state.json entries of all runs within a given suite path.
  *
  * @param suitePath The suite path to be traversed.
  * @param config The application configuration.
  */
class SuiteTraverser(suitePath: Path)(implicit config: Config) extends Traversable[GenericRunState] {

  import SuiteTraverser.loadState

  override def foreach[U](f: GenericRunState => U): Unit = for {
    dir <- suitePath.toFile.listFiles.sortBy(_.getAbsolutePath) if dir.isDirectory
    fil <- Option(new File(dir, "state.json")) if fil.isFile
    run <- loadState(fil)
  } f(run)
}

/** Companion object. */
object SuiteTraverser {

  private implicit val RunStateFormat = RunStateProtocol.stateFormat

  def apply(suitePath: Path)(implicit config: Config) = new SuiteTraverser(suitePath)

  private def loadState(runFile: File): Option[GenericRunState] = {
    val prefix =
      s"""
       |{
       |  "name": "${runFile.getParentFile.getName}",
       |  "suiteName": "${runFile.getParentFile.getParentFile.getName}",
      """.stripMargin.trim
    (for {
      src <- managed(scala.io.Source.fromFile(runFile))
    } yield src.mkString.replaceFirst("""\{""", prefix).parseJson.convertTo[GenericRunState]).opt
  }
}
