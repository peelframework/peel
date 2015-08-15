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
package org.peelframework.core.results.etl

import java.sql.Connection

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.peelframework.core.results.etl.EventExtractorManager.ProcessFile
import org.springframework.context.ApplicationContext

/** A system for [[org.peelframework.core.results.model.ExperimentEvent ExperimentEvent]] extraction. */
class ResultsETLSystem private(context: ApplicationContext, config: Config)(implicit conn: Connection) {

  /** The underlying Akka ecosystem. */
  private val system = ActorSystem("etl", config)

  /** The extractor actor. */
  private val extractor = system.actorOf(EventExtractorManager.props(context, config, conn), "extractor")

  // register system shutdown hook
  sys addShutdownHook {
    system.shutdown()
  }

  /** Process a file asynchronously. */
  def !(msg: ProcessFile): Unit = {
    extractor ! msg
  }

  /** Shutdown the system. */
  def shutdown(): Unit = {
    extractor ! EventExtractorManager.Shutdown
    system.awaitTermination()
  }
}

/** Companion object. */
object ResultsETLSystem extends {

  /** Default constructor. */
  def apply(context: ApplicationContext, config: Config)(implicit conn: Connection): ResultsETLSystem = {
    new ResultsETLSystem(context, config)
  }
}
