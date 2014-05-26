package extensions

import common.{LifecycleElement, Shell, SystemComponent}
import common.SCState._
import common.LifecycleElement

/**
 * Created by felix on 29.04.14.
 */
class Stratosphere(env : {val shell: Shell}) extends SystemComponent{

  private val lifecycle: List[SCState]  = List(init, stop)

  val  getLifecycle: List[LifecycleElement] = {for {lc <- lifecycle} yield LifecycleElement(lc, this.hashCode())}

  def setup() {
    println("setting up hdfs...")
  }

  def tearDown() {
    println("tearing down hdfs...")
  }

  def update(le: LifecycleElement) {

  }
}
