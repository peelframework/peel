package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.beans.ExecutionContext
import scala.collection.JavaConverters._
import java.util
import eu.stratosphere.fab.core.DependencyGraph
import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import org.slf4j.LoggerFactory

/**
 * Created by felix on 02.06.14.
 */
class ExperimentSuite(final val experiments: util.ArrayList[Experiment]) {

  final val expList: List[Experiment] = experiments.asScala.toList
  final val logger = LoggerFactory.getLogger(this.getClass)

  def run() =  {
    val context: ExecutionContext = new ExecutionContext

    val depGraph: DependencyGraph[String] = createGraph()
    
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
  def createGraph(): DependencyGraph[String] = {
    val g = new DependencyGraph[String]

    def getDependencies(s: System): Unit = {
      if(!s.dependencySet.isEmpty) {
        for(d <- s.dependencySet) yield {
          g.addEdge(s.name, d.name)
          getDependencies(d)
        }
      }
    }

    for(e <- expList) yield { // for every experiment
      val r: ExperimentRunner = e.runner // get the experiment runner
      g.addEdge(e.name, r.name) // make an edge from experiment to runner
      getDependencies(r)
    }

    g // return Graph
  }


}
