package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.model.Experiment;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import eu.stratosphere.peel.analyser.model.ExperimentSuite;
import eu.stratosphere.peel.analyser.model.System;
import eu.stratosphere.peel.analyser.parser.ParserSpark;
import eu.stratosphere.peel.analyser.util.HibernateUtil;
import eu.stratosphere.peel.analyser.util.ORM;
import eu.stratosphere.peel.analyser.util.ORMUtil;
import org.easymock.EasyMock;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.PowerMockUtils;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

/**
 * Created by Fabian on 06.01.2015.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest( {ParserManagerHelper.class, ParserManager.class})
public class ParserManagerTest {
  private String path = "path";
  File suiteDirectory = null;
  File experimentDirectory = null;
  File experimentRunDirectory = null;
  File logs = null;
  File stateJson = null;
  File logFile = null;
  System system = null;
  ExperimentSuite experimentSuite = null;
  BufferedReader buf = null;
  FileReader reader = null;
  Experiment experiment = null;
  ParserSpark parserSpark = null;
  ExperimentRun experimentRun = null;

  @Test
  public void testParserManager() throws Exception{
    mockPeelFolder();
    ParserManager parserManager = new ParserManager(path, true);
    parserManager.parsePath();
    verify();
  }

  private File mockPeelFolder() throws Exception{
    suiteDirectory = PowerMock.createMock(File.class);
    experimentDirectory = EasyMock.createNiceMock(File.class);
    experimentRunDirectory = EasyMock.createNiceMock(File.class);
    logs = EasyMock.createNiceMock(File.class);
    stateJson = EasyMock.createNiceMock(File.class);
    reader = PowerMock.createNiceMock(FileReader.class);
    buf = PowerMock.createNiceMock(BufferedReader.class);
    logFile = EasyMock.createNiceMock(File.class);
    system = EasyMock.createNiceMock(System.class);
    experimentSuite = EasyMock.createNiceMock(ExperimentSuite.class);
    experiment = EasyMock.createNiceMock(Experiment.class);
    parserSpark = EasyMock.createNiceMock(ParserSpark.class);
    experimentRun = EasyMock.createNiceMock(ExperimentRun.class);

    ParserSpark parserSpark = PowerMock.createNiceMock(ParserSpark.class);

    PowerMock.expectNew(File.class, path).andReturn(suiteDirectory);
    PowerMock.expectNew(ParserSpark.class, true).andReturn(parserSpark);
    PowerMock.expectNew(FileReader.class, stateJson).andReturn(reader);
    PowerMock.expectNew(FileReader.class, logFile).andReturn(reader);
    PowerMock.expectNew(BufferedReader.class, reader).andReturn(buf).times(2);

    EasyMock.expect(suiteDirectory.isDirectory()).andReturn(true);
    EasyMock.expect(suiteDirectory.listFiles()).andReturn(new File[]{experimentDirectory});

    EasyMock.expect(experimentDirectory.listFiles()).andReturn(new File[]{experimentRunDirectory});

    EasyMock.expect(experimentRunDirectory.listFiles()).andReturn(
                    new File[] { logs, stateJson });

    EasyMock.expect(logs.getName()).andReturn("logs");
    EasyMock.expect(stateJson.getName()).andReturn("state.json");


    EasyMock.expect(buf.readLine()).andReturn("{");
    EasyMock.expect(buf.readLine()).andReturn("\"name\": \"spark.dop320.men14G.run04\",");
    EasyMock.expect(buf.readLine()).andReturn("\"suiteName\": \"tpch3.50G.wally\",");
    EasyMock.expect(buf.readLine()).andReturn("\"command\": \"--class eu.stratosphere.procrustes.experiments.spark.tpch.TPCHQuery3 /home/peel/peel/jobs/procrustes-spark-1.0-SNAPSHOT.jar spark://wally001:7077 hdfs://wally001:45010//tmp/input/lineitem.tbl hdfs://wally001:45010//tmp/input/customer.tbl hdfs://wally001:45010//tmp/input/orders.tbl  hdfs://wally001:45010//tmp/output/tpch3\",");
    EasyMock.expect(buf.readLine()).andReturn("\"runnerName\": \"spark\",");
    EasyMock.expect(buf.readLine()).andReturn("\"runnerVersion\": \"1.1.0\",");
    EasyMock.expect(buf.readLine()).andReturn("\"runExitCode\": 0,");
    EasyMock.expect(buf.readLine()).andReturn("  \"runTime\": 72078");
    EasyMock.expect(buf.readLine()).andReturn("}");
    EasyMock.expect(buf.readLine()).andReturn(null);


    //never accesses ormutil. It's just a placeholder and it's important not to execute the hibernate connection etc. since that won't work with PowerMock runner
    HibernateUtil.ormUtil = EasyMock.createNiceMock(ORM.class);

    PowerMock.mockStaticNice(ParserManagerHelper.class);
    EasyMock.expect(ParserManagerHelper
                                    .isFailedExperimentRun((JSONObject) EasyMock
                                                    .anyObject())).andReturn(
                    false);
    EasyMock.expect(ParserManagerHelper.getSystem("spark", "1.1.0")).andReturn(null);
    EasyMock.expect(ParserManagerHelper.saveSystem(EasyMock.anyObject(
                    JSONObject.class))).andReturn(system);
    EasyMock.expect(ParserManagerHelper.getExperimentSuite("tpch3.50G.wally")).andReturn(null);
    EasyMock.expect(ParserManagerHelper.saveExperimentSuite(
                    EasyMock.anyObject(JSONObject.class))).andReturn(experimentSuite);
    EasyMock.expect(ParserManagerHelper.getExperimentName(
                    EasyMock.anyObject(JSONObject.class))).andReturn("spark.dop320.men14G");
    EasyMock.expect(ParserManagerHelper.getExperiment(
                    EasyMock.anyObject(JSONObject.class))).andReturn(null);
    EasyMock.expect(ParserManagerHelper.saveExperiment("spark.dop320.men14G",
                    experimentSuite, system)).andReturn(experiment);
    EasyMock.expect(ParserManagerHelper.saveExperimentRun(
                    EasyMock.eq(experiment),
                    EasyMock.anyObject(JSONObject.class))).andReturn(
                    experimentRun);
    EasyMock.expect(ParserManagerHelper.getSystemOfExperimentRun(experimentRun)).andReturn("spark");
    EasyMock.expect(ParserManagerHelper.findLogFile(EasyMock.anyObject(File.class), EasyMock.eq("spark"))).andReturn(
                    logFile);

    EasyMock.expect(experimentRun.getExperiment()).andReturn(experiment);
    EasyMock.expect(experiment.getName()).andReturn("");
    EasyMock.expect(experimentRun.getRun()).andReturn(1);

    PowerMock.replayAll(suiteDirectory, parserSpark, buf,
                    reader, ParserManagerHelper.class);
    EasyMock.replay(experimentDirectory, experimentRunDirectory, logs, stateJson, logFile, experimentRun, experiment);
    return suiteDirectory;
  }

  private void verify() throws Exception {
    EasyMock.verify(experimentDirectory, experimentRunDirectory, logs, logFile, experimentRun, experiment);
    PowerMock.verifyAll();
  }



}
