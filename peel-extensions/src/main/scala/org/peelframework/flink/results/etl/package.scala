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
package org.peelframework.flink.results

import java.time.{ZoneOffset, Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

/** Package object. */
package object etl {

  /** Pattern for job output log entries. */
  val LogEntryV1 = """([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3})\t(.+)""".r

  /** Pattern for job output log entries. */
  val LogEntryV2 = """([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}).* - (.+)""".r

  /** Pattern for task state transitions. */
  val TaskStateV1 = """(.+)\((\d+)/(\d+)\) switched to (SCHEDULED|DEPLOYING|RUNNING|FINISHED)\W*""".r

  /** Pattern for task state transitions. */
  val TaskStateV2 = """(.+) \((\d+)/(\d+)\) \((.+)\) switched from \w+ to (SCHEDULED|DEPLOYING|RUNNING|FINISHED)""".r

  private val TimestampFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS")

  def toInstant(v: String): Instant = {
    LocalDateTime.parse(v, TimestampFmt).toInstant(ZoneOffset.UTC)
  }
}
