package eu.stratosphere.peel.core

import eu.stratosphere.peel.core.beans.data.GeneratedDataSet
import eu.stratosphere.peel.core.beans.experiment.ExperimentSuite
import eu.stratosphere.peel.core.beans.system.System
import org.slf4j.LoggerFactory

/** Package object that holds the functionality to create the dependency graph
  * from the specified experiment-fixtures and their dependencies.
  *
  */
package object graph {

  final val logger = LoggerFactory.getLogger(this.getClass)

  /** Create a directed Graph from a system and its subsystems.
   *
   * @return Graph with systems as vertices and dependencies as edges
   */
  def createGraph(system: System): DependencyGraph[Node] = {
    logger.info(s"Constructing dependency graph for system '${system.beanName}'")

    // initial graph
    implicit val g = new DependencyGraph[Node]

    g.addVertex(system)
    processDependencies(system)

    g
  }

  /** Create a directed Graph from all Experiments and their dependencies.
   *
   * @return Graph with systems as vertices and dependencies as edges
   */
  def createGraph(suite: ExperimentSuite): DependencyGraph[Node] = {
    logger.info(s"Constructing dependency graph for suite '${suite.name}'")

    // initial graph
    implicit val g = new DependencyGraph[Node]

    for (e <- suite.experiments) {
      g.addEdge(suite, e)
      // add the experiment runner
      g.addEdge(e, e.runner)
      processDependencies(e.runner)

      // add the experiment inputs and their dependencies
      for (i <- e.inputs) {
        g.addEdge(e, i)
        // add the input dependencies
        for (x <- i.dependencies) {
          g.addEdge(i, x)
          processDependencies(x)
        }
      }
      // add the experiment outputs and their dependencies
      for (o <- e.outputs) {
        g.addEdge(e, o)
        // add the output dependency
        g.addEdge(o, o.fs)
        processDependencies(o.fs)
      }
    }

    // add job dependencies to generated data sets (FIXME: generalize the dependency mechanism)
    g.vertices.collect({
      case i: GeneratedDataSet =>
        g.addEdge(i, i.src)
        g.addEdge(i.src, i.src.runner)
        processDependencies(i.src.runner)
    })

    g // return the graph
  }

  /** helper function: process system dependencies */
  def processDependencies(s: System)(implicit g: DependencyGraph[Node]): Unit = {
    if (s.dependencies.nonEmpty) {
      for (d <- s.dependencies) {
        g.addEdge(s, d)
        processDependencies(d)
      }
    }
  }
}
