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
