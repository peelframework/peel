package eu.stratosphere.fab.core

import scala.collection.mutable.HashMap
import scala.annotation.tailrec

/**
 * Created by felix on 02.06.14.
 */

class DependencyGraph[T] {

  var graph: HashMap[T, Set[T]] = new HashMap[T, Set[T]]()

  def isEmpty: Boolean = graph.isEmpty

  def size: Int = graph.size

  // all vertices as a set
  def vertices: Set[T] = graph.keySet.toSet

  // all edges as a set
  def edges: Set[T] = graph.values.flatten.toSet

  /**
   * adds a vertex
   * if vertex already exists, returns the value associated with it
   * else: add vertex with emoty set as value and return empty
   * @param key vertex to insert
   * @return this vertex-key
   */
  def addVertex(key: T): Set[T] = graph.getOrElseUpdate(key, Set())

  /**
   * adds an edge from src to target
   * if one or both of the vertices do not exist yet, they are created
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
   * adds edges defined in a list
   * @param edges list of edges to be added to the graph
   */
  def addEdge(edges: List[Tuple2[T, T]]): Unit = {
    for ((s, t) <- edges) yield addEdge(s, t)
  }

  /**
   * tests if this vertex has any edge
   * @param src the vertex to check for an edge
   * @return true if vertex has edge, false otherwise
   */
  def hasEdge(src: T): Boolean = {
    graph.get(src) match {
      case Some(s) => if(s == Set()) false else true
      case _ => false
    }
  }

  /**
   * checks if the graph has an edge between two vertices
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
   * determine if the graph has a cycle
   * @return true if graph has a cycle, false otherwise
   */
  def hasCycle: Boolean = ???

  /**
   * topological sort of the directed graph
   * @return sorted Graph (List of names of the nodes)
   */
  def topoSort: List[T] = ???

  /**
   * Depth-First-Search on the Graph
   * If no start vertex is given, all vertices with in-degree
   * zero are selected as start vertices. If a start vertex is given,
   * all vertices that have in-degree zero are added additionally.
   * @param start optional start vertex.
   * @return List of a possible depth first search sequence
   */
  def dfs(start: Set[T] = vertices diff edges): List[T] = {

    @tailrec
    def loop(toVisit: Set[T], visited: List[T]): List[T] = {
      if (toVisit.isEmpty) visited
      else {
        val next: T = toVisit.head
        val children: Set[T] = graph(next) filter (x => !visited.contains(x))

        loop(children ++ toVisit.tail, next :: visited)
      }
    }

    if(start != (vertices diff edges))
      loop(start ++ (vertices diff edges), List()).reverse
    else
      loop(start, List()).reverse
  }

  /**
   * Deth first search that starts at one node and gets all dependencies
   * Does NOT traverse all nodes but only the ones reachable from the start node!
   * @param start node to start with
   * @return sequence of dependencies of that node
   */
  def directDependencies(start: T): List[T] = {
    @tailrec
    def loop(toVisit: Set[T], visited: List[T]): List[T] = {
      if (toVisit.isEmpty) visited
      else {
        val next: T = toVisit.head
        val children: Set[T] = graph(next) filter (x => !visited.contains(x))

        loop(children ++ toVisit.tail, next :: visited)
      }
    }

    loop(Set(start), List()).reverse
  }

  /**
   * reverses the Graph
   * @return new Graph with reversed edges
   */
  def reverse: DependencyGraph[T] = {
    if(!isEmpty) {
      val newGraph = new DependencyGraph[T]()
        for {
          (vertex, neighbours) <- graph.toList
          neighbour <- neighbours
        } yield newGraph.addEdge(neighbour, vertex)
      newGraph
    } else
      throw new Exception("Cannot reverse empty Graph!")
  }

  override def toString: String = {
    (for(v <- vertices; e <- graph.get(v)) yield {v.toString + " --> " + e.toString}).mkString("\n")
  }

  override def equals(that: Any) = that match {
    case other: DependencyGraph[T] => other.graph == this.graph
    case _ => false
  }
}


