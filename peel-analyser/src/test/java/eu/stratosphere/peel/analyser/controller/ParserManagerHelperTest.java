package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.model.*;
import eu.stratosphere.peel.analyser.model.System;
import eu.stratosphere.peel.analyser.util.HibernateUtil;
import eu.stratosphere.peel.analyser.util.ORM;
import eu.stratosphere.peel.analyser.util.ORMUtil;
import org.easymock.EasyMock;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ParserManagerHelperTest {

    @Test
    public void testExperimentName() throws Exception{
        JSONObject jsonObject = EasyMock.createMock(JSONObject.class);
        EasyMock.expect(jsonObject.getString("name")).andReturn("kmeans.D3K8.10m.dop64.men36G.run01");
        EasyMock.replay(jsonObject);

        String result = ParserManagerHelper.getExperimentName(jsonObject);
        assertEquals("kmeans.D3K8.10m.dop64.men36G", result);
    }

    @Test
    public void testIsJobmanager() throws Exception {
        String input = "flink-impro-jobmanager-cloud-7.log";
        boolean result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(true, result);

        input = "flink-impro-jobmanager-cloud-7.out";
        result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(false, result);

        input = "flink-impro-taskmanager-cloud-9.out";
        result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(false, result);

        input = "flink-impro-flink-client-cloud-7";
        result = ParserManagerHelper.isJobmanager(input, "flink");
        assertEquals(false, result);

        input = "app-20150506160041-0000";
        result = ParserManagerHelper.isJobmanager(input, "spark");
        assertEquals(true, result);
    }

    @Test
    public void testIsFailedExperimentRun() throws Exception{
        JSONObject jsonObject = EasyMock.createMock(JSONObject.class);
        EasyMock.expect(jsonObject.get("runExitCode")).andReturn(0);
        EasyMock.replay(jsonObject);

        boolean result = ParserManagerHelper.isFailedExperimentRun(jsonObject);
        assertFalse(result);
        EasyMock.verify();

        jsonObject = EasyMock.createMock(JSONObject.class);
        EasyMock.expect(jsonObject.get("runExitCode")).andReturn(1);
        EasyMock.replay(jsonObject);

        result = ParserManagerHelper.isFailedExperimentRun(jsonObject);
        assertTrue(result);
        EasyMock.verify(jsonObject);
    }

    @Test
    public void testSaveExperimentRun() throws Exception {
        ORM orm = HibernateUtil.getORM();

        Experiment experiment = new Experiment();
        orm.beginTransaction();
        orm.save(experiment);
        orm.commitTransaction();


        String jsonString = "{\n" +
                "  \"name\": \"spark.dop320.men14G.run04\",\n" +
                "  \"suiteName\": \"tpch3.50G.wally\",\n" +
                "  \"command\": \"--class eu.stratosphere.procrustes.experiments.spark.tpch.TPCHQuery3 /home/peel/peel/jobs/procrustes-spark-1.0-SNAPSHOT.jar spark://wally001:7077 hdfs://wally001:45010//tmp/input/lineitem.tbl hdfs://wally001:45010//tmp/input/customer.tbl hdfs://wally001:45010//tmp/input/orders.tbl  hdfs://wally001:45010//tmp/output/tpch3\",\n" +
                "  \"runnerName\": \"spark\",\n" +
                "  \"runnerVersion\": \"1.1.0\",\n" +
                "  \"runExitCode\": 0,\n" +
                "  \"runTime\": 72078\n" +
                "}";
        String jsonString2 = "{\n" +
                "  \"name\": \"spark.dop320.men14G.run05\",\n" +
                "  \"suiteName\": \"tpch3.50G.wally\",\n" +
                "  \"command\": \"--class eu.stratosphere.procrustes.experiments.spark.tpch.TPCHQuery3 /home/peel/peel/jobs/procrustes-spark-1.0-SNAPSHOT.jar spark://wally001:7077 hdfs://wally001:45010//tmp/input/lineitem.tbl hdfs://wally001:45010//tmp/input/customer.tbl hdfs://wally001:45010//tmp/input/orders.tbl  hdfs://wally001:45010//tmp/output/tpch3\",\n" +
                "  \"runnerName\": \"spark\",\n" +
                "  \"runnerVersion\": \"1.1.0\",\n" +
                "  \"runExitCode\": 0,\n" +
                "  \"runTime\": 72078\n" +
                "}";
        JSONObject jsonObject = new JSONObject(jsonString);
        JSONObject jsonObject1 = new JSONObject(jsonString2);

        ParserManagerHelper.saveExperimentRun(experiment, jsonObject);
        ParserManagerHelper.saveExperimentRun(experiment, jsonObject1);
    }

    @Test
    public void testGetExperiment() throws Exception {
        ORM orm = HibernateUtil.getORM();
        orm.beginTransaction();

        String jsonString = "{\n" +
                "  \"name\": \"spark.dop320.men14G.run04\",\n" +
                "  \"suiteName\": \"tpch3.50G.wally\",\n" +
                "  \"command\": \"--class eu.stratosphere.procrustes.experiments.spark.tpch.TPCHQuery3 /home/peel/peel/jobs/procrustes-spark-1.0-SNAPSHOT.jar spark://wally001:7077 hdfs://wally001:45010//tmp/input/lineitem.tbl hdfs://wally001:45010//tmp/input/customer.tbl hdfs://wally001:45010//tmp/input/orders.tbl  hdfs://wally001:45010//tmp/output/tpch3\",\n" +
                "  \"runnerName\": \"spark\",\n" +
                "  \"runnerVersion\": \"1.1.0\",\n" +
                "  \"runExitCode\": 0,\n" +
                "  \"runTime\": 72078\n" +
                "}";

        ExperimentSuite experimentSuite = new ExperimentSuite();
        experimentSuite.setName("tpch3.50G.wally");
        orm.save(experimentSuite);

        eu.stratosphere.peel.analyser.model.System system = new System();
        system.setName("spark");
        system.setVersion("1.1.0");
        orm.save(system);

        orm.commitTransaction();

        ParserManagerHelper.saveExperiment("spark.dop320.men14G", experimentSuite, system);
        ParserManagerHelper.saveExperiment("spark.dop320.men32G", experimentSuite, system);

        Experiment result = ParserManagerHelper.getExperiment(new JSONObject(jsonString));

        assertEquals(experimentSuite.getName(), result.getExperimentSuite().getName());
        assertEquals(system.getName(), result.getSystem().getName());
        assertEquals(system.getVersion(), result.getSystem().getVersion());

    }

    @After
    public void resetDatabase() {
        HibernateUtil.deleteAll();
    }



}