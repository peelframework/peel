/**
 * Created by felix on 27.04.14.
 */
package sketchbook

import extensions.{ExpController, ConfigRepo}


object NoExp extends App{

  override def main(args: Array[String]) {
    val configPath: String = args(0)
    val exp = new ExpController(ConfigRepo, configPath)
    exp.run()
  }
}
