package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.{ExecutionContext, Node, DependencyGraph}
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import org.slf4j.LoggerFactory

/**
 * Created by felix on 02.06.14.
 */
class ExperimentSuite(final val experiments: List[Experiment]) extends Node{

  final val logger = LoggerFactory.getLogger(this.getClass)

  def run() =  {
    val context: ExecutionContext = new ExecutionContext

    val depGraph: DependencyGraph[Node] = createGraph()

    //TODO check for cycles in the graph
    if(!depGraph.isEmpty) {
      logger.info("Succesfulyy created experiment graph: \n " + depGraph)
    }
    else {
      throw new RuntimeException("Could not create Graph! (Graph is empty)")
    }

  }

  /**
   * create a directed Graph from all Experiments and their dependencies
   * @return Graph with systems as vertices and dependencies as edges
   */
  def createGraph(): DependencyGraph[Node] = {
    val g = new DependencyGraph[Node]

    def getDependencies(s: System): Unit = {
      if(!s.dependencies.isEmpty) {
        for(d <- s.dependencies) yield {
          g.addEdge(s, d)
          getDependencies(d)
        }
      }
    }

    for(e <- experiments) yield { // for every experiment
      val r: ExperimentRunner = e.runner // get the experiment runner
      g.addEdge(e, r) // make an edge from experiment to runner
      getDependencies(r)
    }

    g // return Graph
  }


}
