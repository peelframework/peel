/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
