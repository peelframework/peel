/**
 * Created by felix on 27.04.14.
 */
package common

import common.SCState.SCState

trait SystemComponent {
  def getLifecycle: List[LifecycleElement]
  def setup: Unit
  def tearDown: Unit
  def update(le: LifecycleElement): Unit
}