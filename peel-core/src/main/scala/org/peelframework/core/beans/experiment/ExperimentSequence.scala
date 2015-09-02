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

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.peelframework.core.beans.system.System

/** A factory for experiment sequences.
  *
  * @param parameters A sequence of parameter maps.
  * @param prototypes A collection of experiment prototypes. Instantiated once per parameter map.
  */
class ExperimentSequence(
  parameters: ExperimentSequence.Parameters,
  prototypes: Seq[Experiment[System]]) extends Seq[Experiment[System]] {

  import org.peelframework.core.beans.experiment.ExperimentSequence.substituteSequenceParameters

  def this(
    parameters: ExperimentSequence.Parameters,
    prototype : Experiment[System]) = this(parameters, Seq(prototype))

  def this(
    paramName : String,
    paramVals : Seq[Any],
    prototype : Experiment[System]) = this(new ExperimentSequence.SimpleParameters(paramName, paramVals), Seq(prototype))

  def this(
    paramName : String,
    paramVals : Seq[Any],
    prototypes: Seq[Experiment[System]]) = this(new ExperimentSequence.SimpleParameters(paramName, paramVals), prototypes)

  private val experiments = {
    val opts = ConfigRenderOptions.defaults().setOriginComments(false)

    val exps = for {
      map       <- parameters
      prototype <- prototypes
      conf      =  prototype.config.root().render(opts)
      name      =  prototype.name
    } yield {
      val n = substituteSequenceParameters(name)(map)
      val c = ConfigFactory.parseString(substituteSequenceParameters(conf)(map))
      // copy prototype
      prototype.copy(config = c, name = n)
    }

    exps.toList
  }

  override def length: Int = experiments.length

  override def apply(idx: Int): Experiment[System] = experiments(idx)

  override def iterator: Iterator[Experiment[System]] = experiments.iterator
}

object ExperimentSequence {

  val parameter = """__(\S+?)__""".r // non greedy pattern for matching ${<id>} sequences

  /** Substitutes all config parameters `${id}` in `v` with their corresponding values defined in `config`.
    *
    * @param v The string where the values should be substituted.
    * @param map A map with parameters to be substituted.
    * @return The substituted version of v.
    * @throws com.typesafe.config.ConfigException.Missing if value is absent or null
    */
  private def substituteSequenceParameters(v: String)(implicit map: Map[String, Any]) = {
    val keys = (for (m <- parameter findAllMatchIn v) yield m group 1).toSet.toList
    val vals = for (k <- keys) yield map(k)
    (keys.map(k => s"__${k}__") zip vals.map(_.toString)).foldLeft(v) { case (z, (s, r)) => z replaceAllLiterally(s, r) }
  }

  /** Parameter class */
  case class Parameter(name: String, vals: Seq[Any])

  /** Base type for the experiment parameters. */
  abstract class Parameters extends Traversable[Map[String, Any]] {
  }

  /** Iterates over the underlying parameter values and constructs a singleton map for each one.
    *
    * @param paramName Parameter name.
    * @param paramVals Parameter values.
    */
  class SimpleParameters(paramName: String, paramVals: Seq[Any]) extends Parameters {

    require(paramVals.nonEmpty, "All parameter sequences must be non-empty")

    private val N = paramVals.size

    override def foreach[U](f: (Map[String, Any]) => U): Unit = {
      for {
        idx <- 0 until N
        map = Map(paramName -> paramVals(idx))
      } yield f(map)
    }
  }

  /** Iterates over the underlying parameter values in 'zipped' mode.
    *
    * @param parameters A collection of `(paramName, paramVals)`. All value sequences must have the same size.
    */
  class ZippedParameters(parameters: Seq[Parameter]) extends Parameters {

    require(parameters.nonEmpty, "At least one parameter sequence required")
    require(parameters.forall(_.vals.nonEmpty), "All parameter sequences must be non-empty")
    require(parameters.map(_.vals.size).distinct.size == 1, "All parameter sequences must be of the same length")

    private val N = parameters.map(_.vals.size).distinct.head

    override def foreach[U](f: (Map[String, Any]) => U): Unit = {
      for {
        idx <- 0 until N
        map = Map(parameters map { p => p.name -> p.vals(idx) }: _*)
      } yield f(map)
    }
  }

  /** Iterates over the underlying parameter values in 'cross' mode (all combinations).
    *
    * @param parameters A collection of `(paramName, paramVals)`.
    */
  class CrossedParameters(parameters: Seq[Parameter]) extends Parameters {

    require(parameters.nonEmpty, "At least one parameter sequence required")
    require(parameters.forall(_.vals.nonEmpty), "All parameter sequences must be non-empty")

    private val N = parameters.map(_.vals.size).distinct.head

    override def foreach[U](f: (Map[String, Any]) => U): Unit = {
      for (pars <- crossParameters()) f(pars.toMap)
    }

    private def crossParameters(): Seq[Seq[(String, Any)]] = {
      parameters.foldLeft(Seq(Seq.empty[(String, Any)]))((res, par) => for (map <- res; value <- par.vals) yield map :+ par.name -> value)
    }
  }

}