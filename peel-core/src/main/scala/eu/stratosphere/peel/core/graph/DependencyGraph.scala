package eu.stratosphere.peel.core.graph

import scala.annotation.tailrec
import scala.collection.mutable

class DependencyGraph[T] {

  var graph: mutable.HashMap[T, Set[T]] = new mutable.HashMap[T, Set[T]]()

  /**
   * @return True if the graph is empty (i.e. has no nodes).
   */
  def isEmpty: Boolean = graph.isEmpty

  /**
   * @return The number of nodes in the graph
   */
  def size: Int = graph.size

  /**
   * @return All vertices as a set.
   */
  def vertices: Set[T] = graph.keySet.toSet

  /**
   * @return All edge targets as a set.
   */
  def edgeTargets: Set[T] = graph.values.flatten.toSet

  /**
   * Adds a vertex to the graph.
   *
   * If vertex already exists, returns the value associated with it, otherwise adds a vertex with emoty set as value
   * and returns empty.
   *
   * @param key vertex to insert
   * @return this vertex-key
   */
  def addVertex(key: T): Set[T] = graph.getOrElseUpdate(key, Set())

  /**
   * Adds an edge from src to target.
   *
   * If one or both of the vertices do not exist yet, they are created.
   *
   * @param src source vertex
   * @param target target vertex
   */
  def addEdge(src: T, target: T) = {
    if (!graph.contains(src))
      addVertex(src)
    if (!graph.contains(target))
      addVertex(target)

    graph.update(src, graph(src) + target)
  }

  /**
   * Adds edges defined in a list.
   *
   * @param edges A list of edges to be added to the graph.
   */
  def addEdge(edges: List[(T, T)]): Unit = {
    for ((s, t) <- edges) yield addEdge(s, t)
  }

  /**
   * Tests if this vertex has any edge.
   *
   * @param src the vertex to check for an edge
   * @return true if vertex has edge, false otherwise
   */
  def hasEdge(src: T): Boolean = {
    graph.get(src) match {
      case Some(s) => if (s == Set()) false else true
      case _ => false
    }
  }

  /**
   * Checks if the graph has an edge between two vertices
   *
   * @param src the source vertex
   * @param target he target vertex
   * @return true if an edge between src and target exists, false otherwise
   */
  def hasEdge(src: T, target: T): Boolean = {
    graph.get(src) match {
      case Some(s) => s.contains(target)
      case _ => false
    }
  }

  /**
   * Determine if the graph has a cycle
   *
   * @return True if graph has a cycle, false otherwise
   */
  def hasCycle: Boolean = ???

  /**
   * Topological sort of the directed graph
   *
   * @return Topologically Sorted Graph (List of names of the nodes)
   */
  def topologicalSort: List[T] = ???

  /**
   * Depth-first traversal on the graph.
   *
   * If no start vertex is given, all vertices with in-degree zero are selected as start vertices.
   * If a start vertex is given, all vertices that have in-degree zero are added additionally.
   *
   * @param start Optional start vertex.
   * @return List of a possible depth first search sequence
   */
  def traverse(start: Set[T] = vertices diff edgeTargets): List[T] = {

    if (start != (vertices diff edgeTargets))
      collect(start ++ (vertices diff edgeTargets), List()).reverse
    else
      collect(start, List()).reverse
  }

  /**
   * Depth-first list of all node descendants.
   *
   * @param start node to start with
   * @return sequence of dependencies of that node
   */
  def descendants(start: T): List[T] = collect(Set(start), List()).reverse

  /**
   * Reverses the graph.
   *
   * @return A new Graph with the same set of vertices and reversed edges.
   */
  def reverse: DependencyGraph[T] = {
    if (!isEmpty) {
      val newGraph = new DependencyGraph[T]()

      for (vertex <- graph.keySet)
        newGraph.addVertex(vertex)
      for ((vertex, neighbours) <- graph.toList; neighbour <- neighbours)
        newGraph.addEdge(neighbour, vertex)

      newGraph
    } else
      throw new Exception("Cannot reverse empty Graph!")
  }

  /**
   * Collects descendants in a depth-first manner starting from the given set.
   *
   * @param toVisit A set of nodes that are yet to be visited.
   * @param visited A list of already visited nodes.
   * @return
   */
  @tailrec
  private def collect(toVisit: Set[T], visited: List[T]): List[T] = {
    if (toVisit.isEmpty) visited
    else {
      val next: T = toVisit.head
      val children: Set[T] = graph(next) filter (x => !visited.contains(x))

      collect(children ++ toVisit.tail, next :: visited)
    }
  }

  override def toString: String = {
    (for (v <- vertices.toList; e <- graph.get(v)) yield {
      v.toString + " --> " + e.toString
    }).mkString("\n")
  }

  override def equals(that: Any) = that match {
    case other: DependencyGraph[T] => other.graph == this.graph
    case _ => false
  }
}


