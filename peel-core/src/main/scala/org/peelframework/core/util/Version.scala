/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
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

import java.util.regex.Pattern


case class Version(numbers: Array[Int], suffix: String) extends Ordered[Version] {

  override def compare(that: Version): Int = {
    val (numbersL, suffixL) = (this.numbers, this.suffix)
    val (numbersR, suffixR) = (that.numbers, that.suffix)

    var i = 0
    // set index to first non-equal ordinal or length of shortest version string
    while (i < numbersL.length && i < numbersR.length && numbersL(i) == numbersR(i)) {
      i = i + 1
    }

    if (i < numbersL.length && i < numbersR.length) {
      Integer.signum(numbersL(i) - numbersR(i)) // compare first non-equal ordinal number
    }
    else if (numbersL.length != numbersR.length) {
      Integer.signum(numbersL.length - numbersR.length) // one is a prefix of the other, "1.2.3" < "1.2.3.4"
    } else if (suffixL == suffixR) {
      +0 // everything matches
    } else if (suffixL == "-snapshot" || suffixR.isEmpty) {
      -1 // left version is a snapshot or right version is stable
    } else if (suffixR == "-snapshot" || suffixL.isEmpty) {
      +1 // right version is a snapshot or left version is stable
    } else {
      suffixL compareTo suffixR
    }
  }

  /** Returns a traversable over all prefixes for the numeric part of the version in decreasing order.
    *
    * @example
    * {{{
    *    Version("1.2.3-SNAPSHOT").prefixes.map(identity) = List(1.2.3, 1.2, 1)
    * }}}
    */
  def prefixes = new Traversable[String] {
    override def foreach[U](f: (String) => U): Unit = {
      for (i <- numbers.length until 0 by -1) f(numbers.slice(0, i).mkString("."))
    }
  }
}

object Version {

  val pattern = Pattern.compile( """(\d+)(.(\d+))*(-[a-zA-Z0-9\_\.-]+)?""")

  def apply(v: String): Version = {
    require(pattern.matcher(v).matches(), s"Invalid version syntax for string '$v' argument, expected d[.d]*[-{suffix}]?")

    v.splitAt(v.indexOf('-')) match {
      case ("", prefix) => Version(prefix.split('.').map(_.toInt), "")
      case (prefix, suffix) => Version(prefix.split('.').map(_.toInt), suffix.toLowerCase)
    }
  }
}

