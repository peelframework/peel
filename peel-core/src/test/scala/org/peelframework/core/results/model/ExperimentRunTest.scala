package org.peelframework.core.results.model

import java.sql.Connection

import com.typesafe.config.ConfigFactory
import org.peelframework.core.results.DB
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks

/** Unit test for the [[org.peelframework.core.results.model.ExperimentRun ExperimentRun]] model class.
  *
  */
@RunWith(classOf[JUnitRunner])
class ExperimentRunTest extends FunSuite with BeforeAndAfter with PropertyChecks with Matchers {

  implicit var conn: Connection = _

  val sys = System('flink090, 'flink, Symbol("0.9.0"))
  val exp1 = Experiment('exp01, 'suite, sys.id)

  before {
    conn = DB.getConnection("test")(ConfigFactory.parseString(""" app.db.test.url = "jdbc:h2:mem:" """))
    DB.createSchema(silent = true)
    System.insert(sys)
    Experiment.insert(exp1)
  }

  after {
    DB.dropSchema(silent = true)
  }

  test("one experiment run is persisted properly") {
    val x = ExperimentRun(exp1.id, 1, 0, 5L)
    ExperimentRun.insert(x)
    ExperimentRun.selectAll() shouldBe Seq(x)
  }

  test("many experiment runs are persisted properly") {
    val xs = for (i <- 0 until 10) yield ExperimentRun(exp1.id, i, 0, 5L)
    ExperimentRun.insert(xs)
    ExperimentRun.selectAll() shouldBe xs
  }

  test("one experiment run is updated properly") {
    val x = ExperimentRun(exp1.id, 1, 0, 5L)
    ExperimentRun.insert(x)
    val y = x.copy(time = 42L)
    ExperimentRun.update(y)
    ExperimentRun.selectAll() shouldBe Seq(y)
  }

  test("many experiment runs are updated properly") {
    val xs = for (i <- 0 until 10) yield ExperimentRun(exp1.id, i, 0, 5L)
    ExperimentRun.insert(xs)
    val ys = for (x <- xs) yield x.copy(time = 42L)
    ExperimentRun.update(ys)
    ExperimentRun.selectAll() shouldBe ys
  }

  test("one experiment run is deleted properly") {
    val x = ExperimentRun(exp1.id, 1, 255, 5L)
    ExperimentRun.insert(x)
    ExperimentRun.selectAll().size shouldBe 1
    ExperimentRun.delete(x)
    ExperimentRun.selectAll().size shouldBe 0
  }

  test("many experiment runs are deleted properly") {
    val xs = for (i <- 0 until 10) yield ExperimentRun(exp1.id, i, -1, 5L)
    ExperimentRun.insert(xs)
    ExperimentRun.selectAll().size shouldBe 10
    ExperimentRun.delete(xs)
    ExperimentRun.selectAll().size shouldBe 0
  }
}
