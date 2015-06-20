package eu.stratosphere.peel.analyser.model;

import eu.stratosphere.peel.analyser.util.HibernateUtil;
import eu.stratosphere.peel.analyser.util.ORM;
import eu.stratosphere.peel.analyser.util.ORMUtil;
import org.hibernate.Session;
import org.junit.Test;
import static org.junit.Assert.*;


public class TaskTest {
    private ORM orm = HibernateUtil.getORM();

    @Test
    public void testGetTaskInstanceBySubtaskNumber() throws Exception {
        String experimentSuiteName = "wc.wordcount.single-run";
        String experimentName = "wordcount";
        int experimentRuns = 5;
        int experimentRunRun = 1;
        Integer subtaskNumber = 1;
        Integer taskNumberOfSubtask = 1;
        Task task;

        TaskInstance taskInstance;
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
            ExperimentRun experimentRun = new ExperimentRun();
            experimentRun.setExperiment(experiment);
            experimentRun.setRun(experimentRunRun);
            orm.save(experimentRun);
            experiment.getExperimentRunSet().add(experimentRun);


            //create Task and add it to TaskType and ExperimentRun
            task = new Task();
            task.setTaskType("CHAIN");
            task.setExperimentRun(experimentRun);
            task.setNumberOfSubtasks(taskNumberOfSubtask);
            orm.save(task);
            task.getTaskID();
            experimentRun.getTaskSet().add(task);

            //create TaskInstance and add it to the task
            taskInstance = new TaskInstance();
            taskInstance.setTask(task);
            taskInstance.setSubTaskNumber(subtaskNumber);
            orm.save(taskInstance);
            task.getTaskInstances().add(taskInstance);

            //commit the transaction
            orm.commitTransaction();
        } catch (Exception e){
            throw e;
        }

        TaskInstance taskInstanceResult = task.taskInstanceBySubtaskNumber(subtaskNumber);
        assertEquals(taskInstance.getSubTaskNumber() ,taskInstanceResult.getSubTaskNumber());
    }
}