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
package org.peelframework.spark.results

import spray.json._

/** Package object. */
package object etl {

  /** Task information. */
  case class TaskInfo(
    taskId      : Int,
    index       : Int,
    attempt     : Short,
    executorId  : String,
    host        : String,
    locality    : String,
    speculative : Boolean,
    launchTime  : Long,
    finishTime  : Long,
    failed      : Boolean
  )

  /** TaskInfo protocol for JSON conversion */
  object TaskEventProtocol extends DefaultJsonProtocol with NullOptions {
    // jsonFormat with explicit field names since spray cannot correctly decode field names with spaces, e.g. `Task ID`
    implicit val format = jsonFormat(TaskInfo,
      "Task ID"     ,
      "Index"       ,
      "Attempt"     ,
      "Executor ID" ,
      "Host"        ,
      "Locality"    ,
      "Speculative" ,
      "Launch Time" ,
      "Finish Time" ,
      "Failed"      )
  }
}
