/**
 * Created by felix on 27.04.14.
 */
package core

trait SystemComponent {
  def getLifecycle: List[LifecycleElement]
  def setup: Unit
  def tearDown: Unit
  def update(e: ExpEvent): Unit
}