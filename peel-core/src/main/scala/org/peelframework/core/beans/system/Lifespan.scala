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
package org.peelframework.core.beans.system

/** Lisfespan states of a system.
  *
  * Different stages of a system's lifecycle.
  *
  * PROVIDED - the system is provided and is not going to be set up by peel
  * SUITE - The system is set up once for the whole suite and torn down at the end
  * EXPERIMENT - The system is set up and torn down for every single experiment
  * RUN - The system is set up and torn down for every single experiment run
  *
  * Note: If a dependency of the system is changed (restarted with different values)
  * then all it's dependants are also reset, independent of the Lifespan. This is to
  * guarantee that all systems are consistent with the parameter settings.
  */
case object Lifespan extends Enumeration {
  type Lifespan = Value
  // define values in order and use the `<` operator to check for containment
  final val RUN, EXPERIMENT, SUITE, PROVIDED = Value
}
