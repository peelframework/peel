package common

/**
 * Created by felix on 29.04.14.
 */
case object SCState extends Enumeration{
  type SCState = Value
  val none, init, run, stop = Value
}
