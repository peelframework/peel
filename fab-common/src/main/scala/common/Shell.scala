/**
 * Created by felix on 27.04.14.
 */
package common

trait Shell {
  def execute(str: String, logOutput: Boolean): Unit
}
