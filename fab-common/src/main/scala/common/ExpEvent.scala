package common

/**
 * Created by felix on 26.05.14.
 */
trait ExpEvent
case class SetupEvent() extends ExpEvent
case class RunEvent() extends ExpEvent
case class TearDownEvent() extends ExpEvent
case class DataEvent(src: List[String], target: String) extends ExpEvent
