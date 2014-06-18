package eu.stratosphere.fab.core

/**
 * Created by felix on 12.06.14.
 */
class ExecutionContext(val expGraph: DependencyGraph[Node]) {
  val runs: Int = 1

  def setRuns(n: Int): ExecutionContext = new ExecutionContext(expGraph){override val runs = n}
}
