package eu.stratosphere.peel.extensions.spark.results.etl

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import spray.json.DefaultJsonProtocol._
import spray.json._

@RunWith(classOf[JUnitRunner])
class SparkEventsTest extends FlatSpec with Matchers with OptionValues {

  import SparkEventsTest._
  import TaskEventProtocol.format

  "SparkListenerTaskEnd" should "match correctly" in {
    val taskEndEventObj = taskEndEvent.parseJson.asJsObject

    taskEndEventObj.fields should (contain key "Event" and contain key "Task Info")

    for (eventName <- taskEndEventObj.fields.get("Event")) {
      eventName should be (JsString("SparkListenerTaskEnd"))
    }

    for (taskInfo <- taskEndEventObj.fields.get("Task Info"); obj = taskInfo.toJson.convertTo[TaskInfo]) {
      obj.taskId      should be (108)
      obj.index       should be (28)
      obj.attempt     should be (0)
      obj.executorId  should be ("3")
      obj.host        should be ("130.149.249.108")
      obj.locality    should be ("PROCESS_LOCAL")
      obj.speculative should be (false)
      obj.launchTime  should be (1438192916855L)
      obj.finishTime  should be (1438192918771L)
      obj.failed      should be (false)
    }
  }
}

object SparkEventsTest {

  val taskEndEvent =
    """
      |{
      |  "Event":"SparkListenerTaskEnd",
      |  "Stage ID":1,
      |  "Stage Attempt ID":0,
      |  "Task Type":"ResultTask",
      |  "Task End Reason":{
      |    "Reason":"Success"
      |  },
      |  "Task Info":{
      |    "Task ID":108,
      |    "Index":28,
      |    "Attempt":0,
      |    "Launch Time":1438192916855,
      |    "Executor ID":"3",
      |    "Host":"130.149.249.108",
      |    "Locality":"PROCESS_LOCAL",
      |    "Speculative":false,
      |    "Getting Result Time":0,
      |    "Finish Time":1438192918771,
      |    "Failed":false,
      |    "Accumulables":[
      |
      |    ]
      |  },
      |  "Task Metrics":{
      |    "Host Name":"130.149.249.108",
      |    "Executor Deserialize Time":221,
      |    "Executor Run Time":1655,
      |    "Result Size":892,
      |    "JVM GC Time":41,
      |    "Result Serialization Time":0,
      |    "Memory Bytes Spilled":0,
      |    "Disk Bytes Spilled":0,
      |    "Shuffle Read Metrics":{
      |    "Remote Blocks Fetched":26,
      |    "Local Blocks Fetched":8,
      |    "Fetch Wait Time":1,
      |    "Remote Bytes Read":33711,
      |    "Local Bytes Read":9629,
      |    "Total Records Read":3171
      |  }
      |  }
      |}
    """.stripMargin
}
