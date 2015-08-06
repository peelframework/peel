package eu.stratosphere.peel.core.results.etl

import java.sql.Connection

import akka.actor._
import eu.stratosphere.peel.core.results.model.ExperimentEvent
import org.springframework.context.ApplicationContext

import scala.collection.mutable.ArrayBuffer
import scala.language.{existentials, postfixOps}

/** Writer actor. Writes [[ExperimentEvent]] instances to the database. */
class Writer(appContext: ApplicationContext, conn: Connection) extends Actor with ActorLogging {

  import Writer.BatchSize

  /** Keep track of what we're watching. */
  val batch = ArrayBuffer.empty[ExperimentEvent]
  var count = 0

  override def preStart() = {
    log.info(s"Staring Writer")
  }

  override def postStop() = {
    if (batch.nonEmpty) insert()
    log.info(s"Extracted $count events")
    log.info(s"Stopped Writer")
  }

  /** Normal state message handler. */
  override def receive: Receive = {
    case event: ExperimentEvent =>
      count += 1
      batch += event
      if (batch.size >= BatchSize) insert()
  }

  def insert() = {
    try {
      ExperimentEvent.insert(batch.result())(conn)
    } catch {
      case e: Throwable => log.warning(s"SQL Exception on batch insert of ExperimentEvent objects: $e")
    } finally {
      batch.clear()
    }
  }
}

/** Companion object. */
object Writer {

  val BatchSize = 1000

  /** Props constructor. */
  def props(context: ApplicationContext, conn: Connection): Props = {
    Props(new Writer(context, conn: Connection))
  }
}
