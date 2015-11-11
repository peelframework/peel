#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.datagen.flink

import org.apache.commons.math3.distribution.{ZipfDistribution, BinomialDistribution, UniformIntegerDistribution}

object Distributions {

  trait DiscreteDistribution {
    def sample(cumulativeProbability: Double) : Int
  }

  case class DiscreteUniform(k: Int) extends DiscreteDistribution {
    val distribution = new UniformIntegerDistribution(0, k - 1)
    def sample(cp: Double) = distribution.inverseCumulativeProbability(cp)
  }

  case class Binomial(sampleSize: Int, successProbability: Double) extends DiscreteDistribution {
    val distribution = new BinomialDistribution(sampleSize - 1, successProbability)
    def sample(cp: Double) = distribution.inverseCumulativeProbability(cp) - 1
  }

  case class Zipf(sampleSize: Int, exponent: Double) extends DiscreteDistribution {
    val distribution = new ZipfDistribution(sampleSize, exponent)
    def sample(cp: Double) = distribution.inverseCumulativeProbability(cp) - 1
  }
}