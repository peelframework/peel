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
