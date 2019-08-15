/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.core.graph

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.classTag

/** Graph that holds the components of the specified Experiment/Suite with their dependencies
  *
  * The dependency-graph represents dependencies between systems, datasets and experiments.
  * The underlying data structure is a mutable HashMap with keys of type T and values of
  * type Set[T].
  *
  * @tparam T type of the vertices
  */
class DependencyGraph[T: ClassTag] {

  var graph: mutable.HashMap[T, Set[T]] = new mutable.HashMap[T, Set[T]]()

  /** Checks if the graph is empty
   * @return True if the graph is empty (i.e. has no nodes).
   */
  def isEmpty: Boolean = graph.isEmpty

  /** Size of the graph
   * @return The number of nodes in the graph
   */
  def size: Int = graph.size

  /** Get a Set of all vertices
   * @return All vertices as a set.
   */
  def vertices: Set[T] = graph.keySet.toSet

  /** Get a set of all vertices that are targeted by another vertex (Values in the map that represents the graph)
   * @return All edge targets as a set.
   */
  def edgeTargets: Set[T] = graph.values.flatten.toSet

  /** Adds a vertex to the graph.
   *
   * If vertex already exists, returns the value associated with it, otherwise adds a vertex with empty set as value
   * and returns empty.
   *
   * @param key vertex to insert
   * @return this vertex-key
   */
  def addVertex(key: T): Set[T] = graph.getOrElseUpdate(key, Set())

  /** Adds an edge from src to target.
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

  /** Adds edges defined in a list.
   *
   * @param edges A list of edges to be added to the graph.
   */
  def addEdge(edges: List[(T, T)]): Unit = {
    for ((s, t) <- edges) yield addEdge(s, t)
  }

  /** Tests if this vertex has any edge.
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

  /** Checks if the graph has an edge between two vertices
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

  /** Determine if the graph has a cycle
    *
    * NOT IMPLEMENTED!
    *
    * @return True if graph has a cycle, false otherwise
    */
  def hasCycle: Boolean = ???
  // TODO implement check for cycle

  /** Topological sort of the directed graph
    *
    * NOT IMPLEMENTED!
    *
    * @return Topologically Sorted Graph (List of names of the nodes)
    */
  def topologicalSort: List[T] = ???
  // TODO implement topological sort

  /** Depth-first traversal on the graph.
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

  /** Depth-first list of all node descendants.
   *
   * @param start node to start with
   * @param excluded a set of nodes to be excluded from the traversal.
   * @return sequence of dependencies of that node
   */
  def descendants(start: T, excluded: Set[_ <: T] = Set.empty[T]): List[T] = collect(Set(start), List(), excluded).reverse

  /** Reverses the graph.
    *
    * All edges are redirected to point from the target to the source.
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

  /** Collects descendants in a breadth-first manner starting from the given set.
   *
   * @param toVisit A set of nodes that are yet to be visited.
   * @param visited A list of already visited nodes.
   * @param excluded a set of nodes to be excluded from the traversal.
   * @tparam U The type of the excluded set elements.
   */
  @tailrec
  private def collect[U <: T: ClassTag](toVisit: Set[T], visited: List[T], excluded: Set[U] = Set.empty[T]): List[T] = {
    val clazz = classTag[T].runtimeClass
    if (toVisit.isEmpty) visited
    else {
      val next: T = toVisit.head
      val children: Set[T] = graph(next) filter {
        case x if clazz.isInstance(x) => !visited.contains(x) && !excluded.contains(x.asInstanceOf[U])
        case x: Any => !visited.contains(x)
      }

      collect(toVisit.tail ++ children, next :: visited, excluded)
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


