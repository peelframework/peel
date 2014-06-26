package eu.stratosphere.peel.core.beans.system

import java.io.File

import com.samskivert.mustache.Mustache
import eu.stratosphere.peel.core.beans.system.Lifespan._

abstract class ExperimentRunner(name: String, lifespan: Lifespan, dependencies: Set[System], mc: Mustache.Compiler) extends System("stratosphere", lifespan, dependencies, mc) {
  def run(job: String, input: List[File], output: File)
}
