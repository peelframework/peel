package eu.stratosphere.fab.core.beans.experiment

import eu.stratosphere.fab.core.{ExecutionContext, Node, DependencyGraph}
import eu.stratosphere.fab.core.beans.system.{Lifespan, ExperimentRunner, System}
import org.slf4j.LoggerFactory

/**
 * Created by felix on 02.06.14.
 */
class ExperimentSuite(final val experiments: List[Experiment]) extends Node{

  final val logger = LoggerFactory.getLogger(this.getClass)

  def run() =  {

    val depGraph: DependencyGraph[Node] = createGraph()
    val context: ExecutionContext = new ExecutionContext(depGraph)


    //TODO check for cycles in the graph
    if(!depGraph.isEmpty) {
      logger.info("Succesfulyy created experiment graph: \n " + depGraph)
    }
    else {
      throw new RuntimeException("Could not create Graph! (Graph is empty)")
    }

    // setup all systems with suite lifecycle
    setUpSuite(depGraph)
    for(e <- experiments) new ExperimentSequence(e).run(context)

    tearDownSuite(depGraph)

  }

  /**
   * Set up all systems with lifecycle suite in the graph
   * @param g the dependency graph
   * @return nothing
   */
  def setUpSuite(g: DependencyGraph[Node]) = {
    for(s <- g.reverse.dfs()) yield {s match {
      case s: System => if (s.lifespan == Lifespan.SUITE) s.setUp()
      case x => x
    }}
  }

  def tearDownSuite(g: DependencyGraph[Node]) = {
    for(s <- g.reverse.dfs()) yield {s match {
      case s: System => if (s.lifespan == Lifespan.SUITE) s.tearDown()
      case x => x
    }}
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

    for(e <- experiments) yield {
      g.addEdge(this, e)
      val r: ExperimentRunner = e.runner
      g.addEdge(e, r)
      getDependencies(r)
    }

    g // return Graph
  }


}
