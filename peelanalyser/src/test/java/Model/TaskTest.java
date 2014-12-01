package Model;

import Util.HibernateUtil;
import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class TaskTest {

    @Before
    public void setUp() throws Exception {
        HibernateUtil.resetDatabase();
    }

    @Test
    public void testGetTaskInstanceBySubtaskNumber() throws Exception {
        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;
        int experimentRunRun = 1;
        Integer subtaskNumber = 1;
        Integer taskNumberOfSubtask = 1;
        Task task = null;

        List resultSuite = null;
        Iterator<Experiment> resultExperimentIterator = null;
        Session session = null;
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
            ExperimentRun experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(experimentRunRun);
            session.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);


            //create Task and add it to TaskType and ExperimentRun
            task = new Task();
            task.setTaskType("CHAIN");
            task.setExperimentRun(experimentRun);
            task.setNumberOfSubtasks(taskNumberOfSubtask);
            session.save(task);
            task.getTaskID();
            experimentRun.getTaskSet().add(task);

            //create TaskInstance and add it to the task
            taskInstance = new TaskInstance();
            taskInstance.setTask(task);
            taskInstance.setSubTaskNumber(subtaskNumber);
            session.save(taskInstance);
            task.getTaskInstances().add(taskInstance);

            //commit the transaction
            session.getTransaction().commit();
        } catch (Exception e){
            if(session != null) {
                session.getTransaction().rollback();
            }
            throw e;
        }

        TaskInstance taskInstanceResult = task.taskInstanceBySubtaskNumber(subtaskNumber);
        assertEquals(taskInstance ,taskInstanceResult);
    }
}