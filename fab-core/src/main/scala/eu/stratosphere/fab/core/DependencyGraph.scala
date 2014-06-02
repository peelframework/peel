package eu.stratosphere.fab.core

import scala.collection.mutable.{HashMap, Set}
import scala.collection.{immutable, mutable}

/**
 * Created by felix on 02.06.14.
 */
class DependencyGraph {

  var graph: mutable.HashMap[String, Node] = new mutable.HashMap[String, Node]()

  def isEmpty: Boolean = graph.isEmpty

  def size: Int = graph.size

  /**
   * returns all nodes of the graph
   * @return nodes of the graph as a set
   */
  def nodes: immutable.Set[Node] = graph.values.iterator.toSet
  def edges: immutable.Set[Edge] = (for(n <- nodes) yield n.edges).flatMap(x => x)

  /**
   * returns the node for the given name if it exists,
   * creates new node with this name otherwise
   * @param name name of the node
   * @return this node if found, new node with this name instead
   */
  def getNode(name: String): Node = graph.get(name) match {
    case Some(n) => n
    case _ => graph.put(name, new Node(name)); new Node(name)
  }

  /**
   * adds an edge from src to target
   * only single edges allowed
   */
  def addEdge(src: String, target: String) = {
    val s: Node = getNode(src)
    val t: Node = getNode(target)
    if(! s.hasEdge(t))
      s.edges.add(new Edge(s, t))
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
  def reverse: DependencyGraph = {
    if(!isEmpty) {
      val g: DependencyGraph = new DependencyGraph() // start with reversing any node
      for(e <- edges) yield {
        val a = g.getNode(e.src.name)
        val b = g.getNode(e.dest.name)
        g.addEdge(b.name, a.name)
      }
      g
    } else
      throw new Exception("Cannot reverse empty Graph!")
  }

  override def toString() = {
    (for(e <- edges) yield {e.toString()}).mkString("\n")
  }


  /**
   * represents a node in the graph
   */
  class Node(val name: String) {
    var edges: mutable.Set[Edge] = new collection.mutable.HashSet[Edge]()
    var visited: Boolean = false

    /**
     * checks if the node has an edge to the given node n
     * @param n node to check for an edge
     * @return true if edge to n exists, false otherwise
     */
    def hasEdge(n: Node): Boolean = edges.contains(new Edge(this, n))

    override def toString() = {
      "Node: " + name
    }

    override def equals(other: Any): Boolean = other match {
      case that: Node => this.name == that.name
      case _ => false
    }
  }

  /**
   * represents an edge in the graph
   * destinaton is the destination node of this edge, source is the this-node
   */
  class Edge(val src: Node, val dest: Node) {
    override def toString() = src.name + " --> " + dest.name

    override def equals(other: Any): Boolean = other match {
      case that: Edge => this.dest == that.dest && this.src == that.src
      case _ => false
    }
  }

}


