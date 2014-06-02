package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.ExecutionContext
import scala.collection.JavaConverters._
import java.util
import eu.stratosphere.fab.core.DependencyGraph
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, JavaExperimentRunner}

/**
 * Created by felix on 02.06.14.
 */
class ExperimentSuite(final val experiments: util.ArrayList[Experiment]) {

  final val expList: List[Experiment] = experiments.asScala.toList

  def run() =  {
    val context: ExecutionContext = new ExecutionContext
    // convert java list to scala list

    val depGraph: DependencyGraph = createGraph()

  }

  /**
   * create a directed Graph from all Experiments and their dependencies
   * @return Graph with systems as nodes and dependencies as edges
   */
  def createGraph(): DependencyGraph = {
    val g = new DependencyGraph

    for(e <- expList) yield { // for every experiment
      val a = g.getNode(e.name) // put experiment in graph
      val r: ExperimentRunner = e.runner // get the experiment runner
      val b = g.getNode(r.name) // put runner in graph
      g.addEdge(a.name, b.name) // make an edge from experiment to runner

      for(dep <- r.dependencySet)yield{ // for every dependency of the runner
        g.addEdge(r.name, g.getNode(dep.name).name) // make an edge from runner to its dependency
        // This does not recursively get all dependencies but only dependencies from the runner!
        // TODO: make recursive implementation that builds up the graph
      }
    }
    println("Graph: " + g)

    val s: DependencyGraph = g.reverse

    println("Reversed: " + s)

    g // return Graph
  }
}
