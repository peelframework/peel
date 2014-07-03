package eu.stratosphere.peel.core.beans.experiment

import com.typesafe.config.Config
import eu.stratosphere.peel.core.beans.data.{DataSet, ExperimentOutput}
import eu.stratosphere.peel.core.beans.system.ExperimentRunner
import eu.stratosphere.peel.core.graph.Node
import org.slf4j.LoggerFactory

abstract class Experiment[+R <: ExperimentRunner](val runner: R, val runs: Int, val inputs: Set[DataSet], val outputs: Set[ExperimentOutput], val config: Config) extends Node {

  def run(id: Int): Unit

  override def toString = config.getString("experiment.name")
}
