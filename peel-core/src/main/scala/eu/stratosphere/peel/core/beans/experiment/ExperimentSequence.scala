package eu.stratosphere.peel.core.beans.experiment

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import eu.stratosphere.peel.core.beans.system.System

/** A factory for experiment sequences.
  * 
  * @param parameters A collection of `(paramName, paramVals)`. All value sequences must have the same size.
  * @param prototypes A collection of experiment prototypes. Instantiated once per value index.
  */
class ExperimentSequence(
  parameters: Seq[(String, Seq[AnyRef])],
  prototypes: Seq[Experiment[System]]) extends Seq[Experiment[System]] {

  import eu.stratosphere.peel.core.beans.experiment.ExperimentSequence.substituteSequenceParameters

  require(parameters.nonEmpty, "At least one parameter sequence required")
  require(parameters.map(_._2.size).distinct.size == 1, "All parameter sequences must be of the same length")
  require(parameters.forall(_._2.nonEmpty), "All parameter sequences must be non-empty")

  private val N = parameters.map(_._2.size).distinct.head

  def this(
    paramName : String,
    paramVals : Seq[AnyRef],
    prototype : Experiment[System]) = this(Seq((paramName, paramVals)), Seq(prototype))

  def this(
    paramName : String,
    paramVals : Seq[AnyRef],
    prototypes: Seq[Experiment[System]]) = this(Seq((paramName, paramVals)), prototypes)

  def this(
    parameters: Seq[(String, Seq[AnyRef])],
    prototype : Experiment[System]) = this(parameters, Seq(prototype))

  private val experiments = {
    val opts = ConfigRenderOptions.defaults().setOriginComments(false)

    val exps = for {
      idx       <- 0 until N
      map       =  Map(parameters map { case (key, params) => key -> params(idx) }: _*)
      prototype <- prototypes
      conf      =  prototype.config.root().render(opts)
      name      =  prototype.name
    } yield {
      val n = substituteSequenceParameters(name)(map)
      val c = ConfigFactory.parseString(substituteSequenceParameters(conf)(map))
      // copy prototype
      prototype.copy(config = c, name = n)
    }
    exps
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
  private def substituteSequenceParameters(v: String)(implicit map: Map[String, AnyRef]) = {
    val keys = (for (m <- parameter findAllMatchIn v) yield m group 1).toSet.toList
    val vals = for (k <- keys) yield map(k)
    (keys.map(k => s"__${k}__") zip vals.map(_.toString)).foldLeft(v) { case (z, (s, r)) => z replaceAllLiterally(s, r) }
  }

}