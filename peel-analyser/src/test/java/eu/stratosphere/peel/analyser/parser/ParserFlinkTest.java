package eu.stratosphere.peel.analyser.parser;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.*;
import eu.stratosphere.peel.analyser.model.System;
import eu.stratosphere.peel.analyser.util.HibernateUtil;
import eu.stratosphere.peel.analyser.util.ORMUtil;
import eu.stratosphere.peel.analyser.util.QueryParameter;
import junit.framework.TestCase;
import org.easymock.EasyMock;
import org.hibernate.Session;
import org.junit.After;
import org.junit.Test;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParserFlinkTest extends TestCase {

    private ExperimentRun experimentRun;
    private Task taskChain;
    String experimentSuiteName = "wc.wordcount.single-run-run";
    String experimentName = "wordcountRun";
    int experimentRuns = 5;
    int experimentRunRun = 1;
    ORMUtil orm = HibernateUtil.getORM();

    //remember to close session!
    protected void setUp() throws Exception{




        try {
            //create session
            orm.beginTransaction();

            //create Experiment Suite
            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            orm.save(experimentSuite);

            System system = new System();
            system.setName("flink");
            orm.save(system);

            //create Experiment and connect it to ExperimentSuite
            Experiment experiment = new Experiment();
            experiment.setExperimentSuite(experimentSuite);
            experiment.setName(experimentName);
            experiment.setRuns(experimentRuns);
            experiment.setSystem(system);
            system.getExperimentSet().add(experiment);
            orm.save(experiment);
            experimentSuite.getExperimentSet().add(experiment);

            //create ExperimentRun and add it to Experiment
            experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(experimentRunRun);
            orm.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);

        } finally {
            orm.commitTransaction();
        }
    }

    @Test
    public void testParseChain1() throws Exception {

        //setup Mock and Test Data
        String input = "15:34:12,930 INFO  org.apache.flink.runtime.execution.ExecutionStateTransition   - TM: ExecutionState set from STARTING to RUNNING for task CHAIN DataSource (TextInputFormat (hdfs://localhost:9000/tmp/input/hamlet.txt) - UTF-8) -> FlatMap (org.apache.flink.example.java.wordcount.WordCount$Tokenizer) -> Combine(SUM(1)) (1/4)";
        BufferedReader reader = EasyMock.createMock(BufferedReader.class);
        EasyMock.expect(reader.readLine()).andReturn(input);
        EasyMock.expect(reader.readLine()).andReturn(null);
        EasyMock.replay(reader);

        //prepare result
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss,SSS");
        Date date = dateFormat.parse("15:34:12,930");
        Integer subTaskNumber = 1;

        ParserFlink parserFlink = new ParserFlink(experimentRun, HibernateUtil.getSession());
        parserFlink.parse(reader);

        orm.beginTransaction();
        try {

            List<Task> taskListResult = orm.executeQuery(Task.class,
                            "from Task");
            Task taskResult = taskListResult.get(0);
            TaskInstance taskInstanceResult = taskResult.getTaskInstances().iterator().next();

            EasyMock.verify(reader);
            assertEquals(date, taskInstanceResult.getEventByName("startingRunning").getValueTimestamp());
            assertEquals(subTaskNumber, taskInstanceResult.getSubTaskNumber());
            assertEquals(1, taskResult.getTaskInstances().size());
        } finally {
            orm.commitTransaction();
        }
    }

    @Test
    public  void testParseJob1() throws Exception {
        String input1 = "15:32:25,579 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Creating initial execution graph from job graph WordCount Example";
        String input2 = "15:32:25,650 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Scheduling job WordCount Example";
        String input3 = "15:32:27,819 INFO  org.apache.flink.runtime.jobmanager.JobManager                - Status of job WordCount Example(303a5e9e4a389c0044a227d32eec8c00) changed to FINISHED";

        BufferedReader reader = EasyMock.createMock(BufferedReader.class);
        EasyMock.expect(reader.readLine()).andReturn(input1);
        EasyMock.expect(reader.readLine()).andReturn(input2);
        EasyMock.expect(reader.readLine()).andReturn(input3);
        EasyMock.expect(reader.readLine()).andReturn(null);
        EasyMock.replay(reader);

        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss,SSS");
        Date creating = dateFormat.parse("15:32:25,579");
        Date scheduling = dateFormat.parse("15:32:25,650");
        Date finished = dateFormat.parse("15:32:27,819");

        ParserFlink parserFlink = new ParserFlink(experimentRun, HibernateUtil.getSession());
        parserFlink.parse(reader);

        orm.beginTransaction();
        try {

            List<ExperimentRun> experimentRunList = orm.executeQuery(
                            ExperimentRun.class, "from ExperimentRun");
            ExperimentRun experimentRunResult = experimentRunList.get(1);

            EasyMock.verify(reader);
            assertEquals(creating, experimentRunResult.getSubmitTime());
            assertEquals(scheduling, experimentRunResult.getDeployed());
            assertEquals(finished, experimentRunResult.getFinished());
        }finally {
            orm.commitTransaction();
        }
    }

    @Test
    public void testFile() throws Exception {
        ClassLoader classLoader = this.getClass().getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("flink-ubuntu-jobmanager-ubuntu-SVP1321L1EBI");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        ParserFlink parserFlink = new ParserFlink(experimentRun, HibernateUtil.getSession());
        parserFlink.parse(reader);
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss,SSS");

        String query = "select experimentRun from ExperimentRun as experimentRun join experimentRun.experiment as experiment join experiment.system as system join experiment.experimentSuite as experimentSuite where experiment.name = :experimentName AND system.name = :systemName AND experimentSuite.name = :experimentSuiteName";
        orm.beginTransaction();
        List<ExperimentRun> experimentRunList = orm.executeQuery(
                        ExperimentRun.class, query,
                        new QueryParameter("experimentName", experimentName),
                        new QueryParameter("systemName", "flink"),
                        new QueryParameter("experimentSuiteName",
                                        experimentSuiteName));

        orm.commitTransaction();
        ExperimentRun experimentRunDatabase = experimentRunList.get(0);

        //test 1
        Date createScheduledReduce4 = dateFormat.parse("15:32:25,657");
        Date resultScheduledReduce4 = experimentRunDatabase.
                taskByTaskType("Reduce").
                taskInstanceBySubtaskNumber(4).
                getEventByName("created to Scheduled").
                getValueTimestamp();
        assertEquals(createScheduledReduce4, resultScheduledReduce4);

        //test 2
        Date assignedReadyChain3 = dateFormat.parse("15:32:25,660");
        Date resultAssignedReadyChain3 = experimentRunDatabase.
                taskByTaskType("CHAIN").
                taskInstanceBySubtaskNumber(3).
                getEventByName("Assigned to Ready").
                getValueTimestamp();
        assertEquals(assignedReadyChain3, resultAssignedReadyChain3);

        //test 3
        Date startingChain1 = dateFormat.parse("15:32:25,662");
        Date resultStartingChain1 = experimentRunDatabase.
                taskByTaskType("CHAIN").
                taskInstanceBySubtaskNumber(3).
                getEventByName("Starting").
                getValueTimestamp();
        assertEquals(startingChain1, resultStartingChain1);
    }

    @After
    public void deleteDatabaseEntries(){
        HibernateUtil.deleteAll();
    }

}