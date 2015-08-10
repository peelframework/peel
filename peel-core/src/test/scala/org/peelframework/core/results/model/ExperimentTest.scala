package org.peelframework.core.results.model

import java.sql.Connection

import com.typesafe.config.ConfigFactory
import org.peelframework.core.results.DB
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

/** Unit test for the [[org.peelframework.core.results.model.Experiment Experiment]] model class.
  *
  */
@RunWith(classOf[JUnitRunner])
class ExperimentTest extends FunSuite with BeforeAndAfter with PropertyChecks with Matchers {

  implicit var conn: Connection = _

  val sys1 = System('flink090, 'flink, Symbol("0.9.0"))
  val sys2 = System('flink091, 'flink, Symbol("0.9.1"))

  before {
    conn = DB.getConnection("test")(ConfigFactory.parseString(""" app.db.test.url = "jdbc:h2:mem:" """))
    DB.createSchema(silent = true)
    System.insert(sys1 :: sys2 :: Nil)
  }

  after {
    DB.dropSchema(silent = true)
  }

  test("one experiment is persisted properly") {
    val x = Experiment('exp, 'suite, sys1.id)
    Experiment.insert(x)
    Experiment.selectAll() shouldBe Seq(x)
  }

  test("many experiments are persisted properly") {
    val xs = for (i <- 0 until 100) yield Experiment(Symbol(s"exp%03${i}d"), 'suite, sys1.id)
    Experiment.insert(xs)
    Experiment.selectAll() shouldBe xs
  }

  test("one experiment is updated properly") {
    val x = Experiment('exp, 'suite, sys1.id)
    Experiment.insert(x)
    val y = x.copy(system = sys2.id)
    Experiment.update(y)
    Experiment.selectAll() shouldBe Seq(y)
  }

  test("many experiments are updated properly") {
    val xs = for (i <- 0 until 100) yield Experiment(Symbol(s"exp%03${i}d"), 'suite, sys1.id)
    Experiment.insert(xs)
    val ys = for (x <- xs) yield x.copy(system = sys2.id)
    Experiment.update(ys)
    Experiment.selectAll() shouldBe ys
  }

  test("one experiment is deleted properly") {
    val x = Experiment('exp, 'suite, sys1.id)
    Experiment.insert(x)
    Experiment.selectAll().size shouldBe 1
    Experiment.delete(x)
    Experiment.selectAll().size shouldBe 0
  }

  test("many experiments are deleted properly") {
    val xs = for (i <- 0 until 100) yield Experiment(Symbol(s"exp%03${i}d"), 'suite, sys1.id)
    Experiment.insert(xs)
    Experiment.selectAll().size shouldBe 100
    Experiment.delete(xs)
    Experiment.selectAll().size shouldBe 0
  }
}
