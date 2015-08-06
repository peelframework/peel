package eu.stratosphere.peel.extensions.flink.results

import java.time.{ZoneOffset, Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

/** Package object. */
package object etl {

  /** Pattern for job output log entries. */
  val LogEntry = """([0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2})\t(.+)""".r

  /** Pattern for task state transitions. */
  val TaskState = """(.+)\((\d+)/(\d+)\) switched to (SCHEDULED|DEPLOYING|RUNNING|FINISHED)\W*""".r

  private val TimestampFmt = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")

  def toInstant(v: String): Instant = {
    LocalDateTime.parse(v, TimestampFmt).toInstant(ZoneOffset.UTC)
  }
}
