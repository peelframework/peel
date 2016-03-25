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
package org.peelframework.flink.results.etl

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FlinkEventsTest extends FlatSpec with Matchers {

  import FlinkEventsTest._

  "LogEntry with TaskState" should "match correctly" in {
    // task 01
    task01Events(0) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:14,000")
        name   should be ("CHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))")
        number should be ("1")
        total  should be ("40")
        state  should be ("SCHEDULED")
    }
    task01Events(1) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:14,000")
        name   should be ("CHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))")
        number should be ("2")
        total  should be ("40")
        state  should be ("DEPLOYING")
    }
    task01Events(2) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:15,000")
        name   should be ("CHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))")
        number should be ("17")
        total  should be ("40")
        state  should be ("RUNNING")
    }
    task01Events(3) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:17,000")
        name   should be ("CHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))")
        number should be ("39")
        total  should be ("40")
        state  should be ("FINISHED")
    }
    // task 02
    task02Events(0) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:21,000")
        name   should be ("DataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))")
        number should be ("3")
        total  should be ("75")
        state  should be ("SCHEDULED")
    }
    task02Events(1) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:21,000")
        name   should be ("DataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))")
        number should be ("3")
        total  should be ("75")
        state  should be ("DEPLOYING")
    }
    task02Events(2) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:55:25,000")
        name   should be ("DataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))")
        number should be ("15")
        total  should be ("75")
        state  should be ("RUNNING")
    }
    task02Events(3) match {
      case LogEntryV1(time, TaskStateV1(name, number, total, state)) =>
        time   should be ("2015-07-29 19:56:30,000")
        name   should be ("DataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))")
        number should be ("20")
        total  should be ("75")
        state  should be ("FINISHED")
    }
    // task 03
    task03Events(0) match {
      case LogEntryV2(time, TaskStateV2(name, number, total, state)) =>
        time   should be ("2016-03-25 15:08:04,996")
        name   should be ("CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33))")
        number should be ("2")
        total  should be ("8")
        state  should be ("SCHEDULED")
    }
    task03Events(1) match {
      case LogEntryV2(time, TaskStateV2(name, number, total, state)) =>
        time   should be ("2016-03-25 15:08:04,996")
        name   should be ("CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33))")
        number should be ("2")
        total  should be ("8")
        state  should be ("DEPLOYING")
    }
    task03Events(2) match {
      case LogEntryV2(time, TaskStateV2(name, number, total, state)) =>
        time   should be ("2016-03-25 15:08:05,014")
        name   should be ("CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33))")
        number should be ("2")
        total  should be ("8")
        state  should be ("RUNNING")
    }
    task03Events(3) match {
      case LogEntryV2(time, TaskStateV2(name, number, total, state)) =>
        time   should be ("2016-03-25 15:08:34,442")
        name   should be ("CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33))")
        number should be ("2")
        total  should be ("8")
        state  should be ("FINISHED")
    }
  }
}

object FlinkEventsTest {

  val task01Events = Seq(
    "2015-07-29 19:55:14,000\tCHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))(1/40) switched to SCHEDULED ",
    "2015-07-29 19:55:14,000\tCHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))(2/40) switched to DEPLOYING ",
    "2015-07-29 19:55:15,000\tCHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))(17/40) switched to RUNNING ",
    "2015-07-29 19:55:17,000\tCHAIN DataSource (at org.peelframework.flink.Wordcount$.main(Wordcount.scala:18) (org.apache.flink.api.java.io.TextInputFormat)) -> FlatMap (FlatMap at org.peelframework.flink.Wordcount$.main(Wordcount.scala:19)) -> Map (Map at org.peelframework.flink.Wordcount$.main(Wordcount.scala:20)) -> Combine(SUM(1))(39/40) switched to FINISHED "
  )

  val task02Events = Seq(
    "2015-07-29 19:55:21,000\tDataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))(3/75) switched to SCHEDULED ",
    "2015-07-29 19:55:21,000\tDataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))(3/75) switched to DEPLOYING ",
    "2015-07-29 19:55:25,000\tDataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))(15/75) switched to RUNNING ",
    "2015-07-29 19:56:30,000\tDataSink (CsvOutputFormat (path: hdfs://wally091:45010/tmp/output/wordcount, delimiter: ,))(20/75) switched to FINISHED "
  )

  val task03Events = Seq(
    "2016-03-25 15:08:04,996 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33)) (2/8) (3a950210daeffb04ddb2667821fcb0ae) switched from CREATED to SCHEDULED",
    "2016-03-25 15:08:04,996 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33)) (2/8) (3a950210daeffb04ddb2667821fcb0ae) switched from SCHEDULED to DEPLOYING",
    "2016-03-25 15:08:05,014 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33)) (2/8) (3a950210daeffb04ddb2667821fcb0ae) switched from DEPLOYING to RUNNING",
    "2016-03-25 15:08:34,442 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - CHAIN DataSource (at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:30) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Map (Map at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:31)) -> Combine (Reduce at de.tu_berlin.dima.experiments.flink.hashagg.flink.WorkloadA$.main(WorkloadA.scala:33)) (2/8) (3a950210daeffb04ddb2667821fcb0ae) switched from RUNNING to FINISHED"
  )
}
