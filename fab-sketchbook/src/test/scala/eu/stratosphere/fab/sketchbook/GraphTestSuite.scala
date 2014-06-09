package eu.stratosphere.fab.sketchbook

import org.scalatest.FunSuite
import eu.stratosphere.fab.core.DependencyGraph
import scala.collection.mutable

class GraphTestSuite extends FunSuite {

  var g = new DependencyGraph[String]

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
    assert(g.addVertex("vertex1") == mutable.Set())
  }

  test("insterting the same element twice should change nothing") {
    g.addVertex("vertex1")
    assert(g.graph == mutable.HashMap("vertex1" -> mutable.Set()))
  }

  test("hasEgde should be false for a single vertex") {
    assert(!g.hasEdge("vertex1"))
  }

  test("adding another vertex and an edge between both should lead to true for hasEdge") {
    g.addEdge("vertex1", "vertex2")
    assert(g.hasEdge("vertex1", "vertex2"))
  }

  test("reverse should do what it should!") {
    val h = new DependencyGraph[String]
    val i = new DependencyGraph[String]
    h.addEdge(List(("vertex1", "vertex2"), ("vertex1", "vertex3"), ("vertex3", "vertex4")))
    i.addEdge(List(("vertex2", "vertex1"), ("vertex4", "vertex3"), ("vertex3", "vertex1")))
    assert(h.reverse == i)
  }


}