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
