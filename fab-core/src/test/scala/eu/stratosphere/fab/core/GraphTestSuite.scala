package eu.stratosphere.fab.core

import org.scalatest.{Matchers, FunSuite}
import scala.collection.mutable

class GraphTestSuite extends FunSuite with Matchers {

  var g = new DependencyGraph[String]
  val i = new DependencyGraph[String]
  i.addEdge(List(("A", "B"), ("B", "C"), ("D", "E"), ("E", "C")))

  test("An empty Graph should have size 0") {
    assert(g.size == 0)
  }

  test("An empty graph should be empty") {
    assert(g.isEmpty)
  }

  test("Invoking reverse on an empty Set should produce Exception") {
    intercept[Exception] {
      g.reverse
    }
  }

  test("inserting an element into the graph should return an empty Set") {
    assert(g.addVertex("A") == mutable.Set())
  }

  test("insterting the same element twice should change nothing") {
    g.addVertex("A")
    assert(g.graph == mutable.HashMap("A" -> mutable.Set()))
  }

  test("hasEgde should be false for a single vertex") {
    assert(!g.hasEdge("A"))
  }

  test("adding another vertex and an edge between both should lead to true for hasEdge") {
    g.addEdge("A", "B")
    assert(g.hasEdge("A", "B"))
  }

  test("reverse should do what it should!") {
    val h = new DependencyGraph[String]
    h.addEdge(List(("C", "B"), ("B", "A"), ("C", "E"), ("E", "D")))
    assert(h.reverse == i)
  }

  test("depth first search with no start vertex given") {
    val l1 = List("A", "B", "C", "D", "E")
    val l2 = List("D", "E", "C", "A", "B")

    List(l1, l2) should contain (i.dfs())
  }

  test("dfs on reversed graph with no start vertex given") {
    val l1 = List("C", "B", "A", "E", "D")
    val l2 = List("C", "E", "D", "B", "A")

    List(l1, l2) should contain (i.reverse.dfs())
  }

  test("dfs with vertex A as start vertex") {
    val l1 = List("A", "B", "C", "D", "E")

    assert(i.dfs(Set("A")) == l1)
  }

  test("dfs with vertex D as start vertex") {
    val l1 = List("D", "E", "C", "A", "B")

    assert(i.dfs(Set("D")) == l1)
  }

  test("should not detect cycle when thre is no cycle") {
    //assert(!i.hasCycle)
  }

  test("should detect cycle when thre is one") {
    //i.addEdge("C", "A")
    //assert(i.hasCycle)
  }

  test("find direct dependencies of A") {
    assert(i.directDependencies("A") == List("A", "B", "C"))
  }

  test("find direct dependencies of D") {
    assert(i.directDependencies("D") == List("D", "E", "C"))
  }

  test("find direct dependencies of C in reversed graph") {
    val l1 = List("C", "B", "A", "E", "D")
    val l2 = List("C", "E", "D", "B", "A")

    List(l1, l2) should contain (i.reverse.directDependencies("C"))
  }



}