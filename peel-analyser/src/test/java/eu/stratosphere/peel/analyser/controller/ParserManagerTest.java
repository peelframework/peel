package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.parser.ParserSpark;
import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.PowerMockUtils;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

/**
 * Created by Fabian on 06.01.2015.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ParserManager.class, ParserSpark.class})
public class ParserManagerTest {
  private String path = "path";
  File suiteDirectory = null;
  File experimentDirectory = null;
  File experimentRunDirectory = null;
  File logs = null;
  File stateJson = null;
  File logFile = null;
  BufferedReader buf = null;
  FileReader reader = null;

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

    ParserSpark parserSpark = PowerMock.createNiceMock(ParserSpark.class);

    PowerMock.expectNew(File.class, path).andReturn(suiteDirectory);
    PowerMock.expectNew(ParserSpark.class, true).andReturn(parserSpark);
    PowerMock.expectNew(FileReader.class, stateJson).andReturn(reader);
    PowerMock.expectNew(FileReader.class, logFile).andReturn(reader);
    PowerMock.expectNew(BufferedReader.class, reader).andReturn(buf).times(2);

    EasyMock.expect(suiteDirectory.isDirectory()).andReturn(true);
    EasyMock.expect(suiteDirectory.listFiles()).andReturn(new File[]{experimentDirectory});

    EasyMock.expect(experimentDirectory.listFiles()).andReturn(new File[]{experimentRunDirectory});

    EasyMock.expect(experimentRunDirectory.listFiles()).andReturn(new File[]{logs, stateJson}).times(2);

    EasyMock.expect(logs.getName()).andReturn("logs").times(2);
    EasyMock.expect(logs.listFiles()).andReturn(new File[]{logFile});
    EasyMock.expect(stateJson.getName()).andReturn("state.json").times(2);

    EasyMock.expect(logFile.getName()).andReturn("EVENT_LOG_1");

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

    PowerMock.replayAll(suiteDirectory, parserSpark, buf, reader);
    EasyMock.replay(experimentDirectory, experimentRunDirectory, logs, stateJson, logFile);
    return suiteDirectory;
  }

  private void verify() throws Exception {
    EasyMock.verify(experimentDirectory, experimentRunDirectory, logs, logFile);
    PowerMock.verifyAll();
  }


}
