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
package org.peelframework.core.beans.experiment

import org.peelframework.core.beans.system.System
import org.peelframework.core.graph.Node
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

/** ExperimentSuite holding a list of experiments
 *
 * @param experiments the experiments contained in that suite.
 */
class ExperimentSuite(final val experiments: Seq[Experiment[System]]) extends Node with BeanNameAware {

  final val logger = LoggerFactory.getLogger(this.getClass)

  var name = "experiments"

  /**
   * Bean name setter.
   *
   * @param n The configured bean name
   */
  override def setBeanName(n: String) = name = n

  /**
   * Alias of name.
   *
   * @return
   */
  override def toString = name
}
