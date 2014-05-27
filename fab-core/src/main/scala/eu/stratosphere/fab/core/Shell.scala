/**
 * Created by felix on 27.04.14.
 */
package core

trait Shell {
  def execute(str: String, logOutput: Boolean): Unit
}
