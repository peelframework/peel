package Parser;

import Model.*;
import Model.System;
import Util.HibernateUtil;
import junit.framework.TestCase;
import org.hibernate.Session;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ParserFlinkTest extends TestCase {

    private ExperimentRun experimentRun;
    private Task taskChain;
    private Session session = null;

    //remember to close session!
    protected void setUp() throws Exception{
        HibernateUtil.resetDatabase();

        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;
        int experimentRunRun = 1;

        List resultSuite = null;
        Iterator<Experiment> resultExperimentIterator = null;
        TaskInstance taskInstance = null;
        try {
            //create session
            session = HibernateUtil.getSessionFACTORY().openSession();
            session.beginTransaction();

            //create Experiment Suite
            ExperimentSuite experimentSuite = new ExperimentSuite();
            experimentSuite.setName(experimentSuiteName);
            session.save(experimentSuite);

            System system = new System();
            system.setName("flink");
            session.save(system);

            //create Experiment and connect it to ExperimentSuite
            Experiment experiment = new Experiment();
            experiment.setExperimentSuite(experimentSuite);
            experiment.setName(experimentName);
            experiment.setRuns(experimentRuns);
            experiment.setSystem(system);
            system.getExperimentSet().add(experiment);
            session.save(experiment);
            experimentSuite.getExperimentSet().add(experiment);

            //create ExperimentRun and add it to Experiment
            experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(experimentRunRun);
            session.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);


            //create Task and add it to TaskType and ExperimentRun
            //taskChain = new Task("CHAIN", experimentRun);
            //session.save(taskChain);
            //experimentRun.getTaskSet().add(taskChain);

            //commit the transaction
            session.getTransaction().commit();
            session.close();        //close session!
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }
    }

    /*@Test
    public void testParseChain1() throws Exception {
        //open new session
        session = HibernateUtil.getSessionFACTORY().openSession();

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

        ParserFlink parserFlink = new ParserFlink(experimentRun, session);
        parserFlink.parse(reader);

        List<Task> taskListResult = session.createQuery("from Task").list();
        Task taskResult = taskListResult.get(0);
        TaskInstance taskInstanceResult = taskResult.getTaskInstances().iterator().next();

        EasyMock.verify(reader);
        assertEquals(date, taskInstanceResult.getEventByName("startingRunning").getValueTimestamp());
        assertEquals(subTaskNumber, taskInstanceResult.getSubTaskNumber());
        assertEquals(1, taskResult.getTaskInstances().size());
        session.close();
    }

    @Test
    public  void testParseJob1() throws Exception {
        session = HibernateUtil.getSessionFACTORY().openSession();
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

        ParserFlink parserFlink = new ParserFlink(experimentRun, session);
        parserFlink.parse(reader);

        List<ExperimentRun> experimentRunList = session.createQuery("from ExperimentRun").list();
        ExperimentRun experimentRunResult = experimentRunList.get(1);

        EasyMock.verify(reader);
        assertEquals(creating, experimentRunResult.getSubmitTime());
        assertEquals(scheduling, experimentRunResult.getDeployed());
        assertEquals(finished, experimentRunResult.getFinished());
        session.close();
    }*/

    @Test
    public void testFile() throws Exception {
        session = HibernateUtil.getSessionFACTORY().openSession();
        BufferedReader reader = new BufferedReader(new FileReader(new File("./src/test/resources/flink-ubuntu-jobmanager-ubuntu-SVP1321L1EBI")));
        ParserFlink parserFlink = new ParserFlink(experimentRun, session);
        parserFlink.parse(reader);
        DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss,SSS");

        List<ExperimentRun> experimentRunList = session.createQuery("from ExperimentRun ").list();
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

}