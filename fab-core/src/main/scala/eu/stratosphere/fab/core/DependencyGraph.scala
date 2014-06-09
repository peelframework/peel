package eu.stratosphere.fab.core

import scala.collection.mutable.HashMap

/**
 * Created by felix on 02.06.14.
 */

class DependencyGraph[T] {

  var graph: HashMap[T, Set[T]] = new HashMap[T, Set[T]]()

  def keys = graph.keySet

  def isEmpty: Boolean = graph.isEmpty

  def size: Int = graph.size

  /**
   * returns all vertices of the graph
   * @return nodes of the graph as a set
   */
  def vertices: Set[T] = graph.keySet.toSet

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
   * topological sort of the directed graph
   * @return sorted Graph (List of names of the nodes)
   */
  def topoSort: List[String] = ???

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

  override def toString() = {
    (for(v <- keys; e <- graph.get(v)) yield {v.toString() + " --> " + e.toString()}).mkString("\n")
  }

  override def equals(that: Any) = that match {
    case other: DependencyGraph[T] => other.graph == this.graph
    case _ => false
  }
}


