package org.peelframework.core.util

object console {

  implicit class ConsoleColorise(val str: String) extends AnyVal {

    import Console._

    def black = s"$BLACK$str$RESET"

    def red = s"$RED$str$RESET"

    def green = s"$GREEN$str$RESET"

    def yellow = s"$YELLOW$str$RESET"

    def blue = s"$BLUE$str$RESET"

    def magenta = s"$MAGENTA$str$RESET"

    def cyan = s"$CYAN$str$RESET"

    def white = s"$WHITE$str$RESET"

    def blackBg = s"$BLACK_B$str$RESET"

    def redBg = s"$RED_B$str$RESET"

    def greenBg = s"$GREEN_B$str$RESET"

    def yellowBg = s"$YELLOW_B$str$RESET"

    def blueBg = s"$BLUE_B$str$RESET"

    def magentaBg = s"$MAGENTA_B$str$RESET"

    def cyanBg = s"$CYAN_B$str$RESET"

    def whiteBg = s"$WHITE_B$str$RESET"
  }

}
