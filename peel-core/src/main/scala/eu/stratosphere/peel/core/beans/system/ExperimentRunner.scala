package eu.stratosphere.peel.core.beans.system

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan._

abstract class ExperimentRunner(name: String, lifespan: Lifespan, dependencies: Set[System], mc: Mustache.Compiler) extends System("stratosphere", lifespan, dependencies, mc) {
  def run(command: String, outFile: String, errFile: String): Int
}
