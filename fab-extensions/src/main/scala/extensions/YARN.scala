package extensions

import common.{LifecycleElement, SystemComponent, Shell}
import common.SCState._
import org.slf4j.LoggerFactory

/**
 * Created by felix on 20.05.14.
 *
 * Represents the Hadoop Yarn system
 */
class YARN(env: { val shell: Shell
                  val hdfs: HDFS}) extends SystemComponent{
  // describes the lifecycle of this system
  private val lifecycle: List[SCState]  = List(init, stop)
  private val logger = LoggerFactory.getLogger(this.getClass)

  val  getLifecycle: List[LifecycleElement] = for {lc <- lifecycle} yield LifecycleElement(lc, this.hashCode())

  def setup() = {

  }

  def tearDown() = {

  }

  def update(le: LifecycleElement) = {

  }

}
