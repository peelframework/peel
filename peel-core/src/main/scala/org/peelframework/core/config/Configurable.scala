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
package org.peelframework.core.config

import com.typesafe.config.Config

trait Configurable {

  /** The Config instance associated with the object. */
  var config: Config

  /** Substitutes all config parameters `\${id}` in `v` with their corresponding values defined in the enclosing `config`.
    *
    * @param v The string where the values should be substituted.
    * @return The subsituted version of v.
    * @throws com.typesafe.config.ConfigException.Missing if value is absent or null
    */
  def resolve(v: String) = substituteConfigParameters(v)(config)
}

object Configurable {

  def unapply(x: Any): Option[Configurable] = x match {
    case s: Configurable => Some(s)
    case _ => None
  }
}