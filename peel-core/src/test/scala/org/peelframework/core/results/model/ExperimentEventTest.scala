/**
 * Copyright (C) 2014 TU Berlin (alexander.alexandrov@tu-berlin.de)
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
package org.peelframework.core.results.model

import java.sql.Connection

import com.typesafe.config.ConfigFactory
import org.peelframework.core.results.DB
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.PropertyChecks
import java.time.Instant

/** Unit test for the [[org.peelframework.core.results.model.ExperimentEvent ExperimentEvent]] model class.
  *
  */
@RunWith(classOf[JUnitRunner])
class ExperimentEventTest extends FunSuite with BeforeAndAfter with PropertyChecks with Matchers {

  implicit var conn: Connection = _

  val sys = System('flink090, 'flink, Symbol("0.9.0"))
  val exp = Experiment('exp01, 'suite, sys.id)
  val run = ExperimentRun(exp.id, 1, 0, 5L)

  before {
    conn = DB.getConnection("test")(ConfigFactory.parseString(""" app.db.test.url = "jdbc:h2:mem:" """))
    DB.createSchema(silent = true)
    System.insert(sys)
    Experiment.insert(exp)
    ExperimentRun.insert(run)
  }

  after {
    DB.dropSchema(silent = true)
  }

  test("one experiment event is persisted properly") {
    val x = ExperimentEvent(experimentRunID = run.id, name = 'MEMORY_USED, vDouble = Some(5.0))
    ExperimentEvent.insert(x)
    ExperimentEvent.selectAll() shouldBe Seq(x)
  }

  test("many experiment events are persisted properly") {
    val xs = eventSequence(100)
    ExperimentEvent.insert(xs)
    ExperimentEvent.selectAll().toSet shouldBe xs.toSet
  }


    test("one experiment event is updated properly") {
      val x = ExperimentEvent(experimentRunID = run.id, name = 'MEMORY_USED, vDouble = Some(5.0))
      ExperimentEvent.insert(x)
      val y = x.copy(vDouble = Some(6.0))
      intercept[NotImplementedError] {
        ExperimentEvent.update(y)
      }
    }

    test("many experiment events are updated properly") {
      val xs = eventSequence(100)
      ExperimentEvent.insert(xs)
      val ys = for (x <- xs) yield x.copy(vString = Some("foo"))
      intercept[NotImplementedError] {
        ExperimentEvent.update(ys)
      }
    }

    test("one experiment event is deleted properly") {
      val x = ExperimentEvent(experimentRunID = run.id, name = 'MEMORY_USED, vDouble = Some(5.0))
      ExperimentEvent.insert(x)
      ExperimentEvent.selectAll().size shouldBe 1
      ExperimentEvent.delete(x)
      ExperimentEvent.selectAll().size shouldBe 0
    }

    test("many experiment events are deleted properly") {
      val xs = eventSequence(100)
      ExperimentEvent.insert(xs)
      ExperimentEvent.selectAll().size shouldBe 100
      ExperimentEvent.delete(xs)
      ExperimentEvent.selectAll().size shouldBe 0
    }

  private def eventSequence(size: Int) = {
    for (i <- 0 until size; j = i % 4) yield j match {
      case 0 => ExperimentEvent(
        experimentRunID = run.id,
        name = 'MEMORY_USED,
        vDouble = Some(i + 5.0))
      case 1 => ExperimentEvent(
        experimentRunID = run.id,
        name = 'TASK_CREATED,
        task = Some("CHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))"),
        taskInstance = Some(i),
        vTimestamp = Some(Instant.now()))
      case 2 => ExperimentEvent(
        experimentRunID = run.id,
        name = 'TASK_STATUS_CHANGE,
        task = Some("task1"),
        taskInstance = Some(i),
        vString = Some("RUNNING"))
      case 3 => ExperimentEvent(
        experimentRunID = run.id,
        name = 'TASK_EXIT,
        task = Some("task1"),
        taskInstance = Some(i),
        vLong = Some(42))
    }
  }
}
