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
package org.peelframework.core.beans.data

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.system.{FileSystem, System}
import org.peelframework.core.config.Configurable
import org.peelframework.core.graph.Node
import org.peelframework.core.util.console._
import org.slf4j.LoggerFactory

/** Represents an abstract Dataset that can be used in experiments
  *
  * This bean is an abstract representation of a dataset and implements the functionality to
  * physically materialize the data that it represents.
  *
  * @param path         Path/location where the data is physically materialized
  * @param dependencies System-Dependencies needed to materialize the dataset (Data-Generators, Systems, ...)
  */
abstract class DataSet(val path: String, val dependencies: Set[System]) extends Node with Configurable {

  final val logger = LoggerFactory.getLogger(this.getClass)

  override var config = ConfigFactory.empty()

  /** The underlying FileSystem. */
  val fs: System with FileSystem

  /** Ensure that the DataSet exists. */
  def ensureExists(): Unit = {
    val p = resolve(path)
    if (!fs.exists(p)) {
      try {
        materialize()
      } catch {
        case e: Throwable =>
          fs.rmr(p) // make sure the path is cleaned for the next try
          throw e
      }
    } else {
      logger.info(s"Skipping already materialized path '$p'".yellow)
    }
  }

  /** Create the data set represented by this bean. */
  def materialize(): Unit // TODO: make protected

  /** Alias of name. */
  override def toString: String = resolve(path)
}
