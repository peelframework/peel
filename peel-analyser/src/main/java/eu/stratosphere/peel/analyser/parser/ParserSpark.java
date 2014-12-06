package eu.stratosphere.peel.analyser.parser;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import eu.stratosphere.peel.analyser.model.Task;
import eu.stratosphere.peel.analyser.model.TaskInstance;
import eu.stratosphere.peel.analyser.model.TaskInstanceEvents;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Date;

/**
 * Created by Fabian on 02.11.2014.
 */
public class ParserSpark implements Parser{
    private ExperimentRun experimentRun;
    private Session session = null;
    private Date firstEntry = null;
    private Date lastEntry = null;
    private boolean skipInstances;

    public ParserSpark(boolean skipInstances) {
        this.skipInstances = skipInstances;
    }

    public ParserSpark(ExperimentRun experimentRun, Session session) {
        this.session = session;
        this.experimentRun = experimentRun;
    }

    public ExperimentRun getExperimentRun() {
        return experimentRun;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }
    public void setExperimentRun(ExperimentRun experimentRun) {
        this.experimentRun = experimentRun;
    }

    public void parse(BufferedReader in) throws IOException, PeelAnalyserException {
        Task task;
        Transaction transaction = session.beginTransaction();

        String line;
        while((line = in.readLine()) != null){
            if(ParserSparkHelper.getEvent(line).equals("SparkListenerTaskEnd") && !skipInstances){
                task = getTaskByLine(line);
                task.getTaskInstances().add(getTaskInstanceByLine(line, task));
                session.update(experimentRun);
            } else if(ParserSparkHelper.getEvent(line).equals("SparkListenerApplicationStart")){
                setSubmitTime(line);
            } else if(ParserSparkHelper.getEvent(line).equals("SparkListenerTaskEnd") && skipInstances){
                if(lastEntry == null){
                    lastEntry = ParserSparkHelper.getFinishTime(line);
                } else if(lastEntry.getTime() < ParserSparkHelper.getFinishTime(line).getTime()) {
                    lastEntry = ParserSparkHelper.getFinishTime(line);
                }
                if(firstEntry == null){
                    firstEntry = ParserSparkHelper.getLaunchTime(line);
                }
            }
        }
        experimentRun.setDeployed(firstEntry);
        experimentRun.setFinished(lastEntry);
        session.update(experimentRun);
        transaction.commit();
    }

    /**
     * this method will parse the line and will create a new TaskInstance object with the parsed information
     * @param input a logfile line
     * @return TaskInstance with all related events
     */
    private TaskInstance getTaskInstanceByLine(String input, Task task){
        TaskInstance taskInstance = new TaskInstance();
        taskInstance.setTask(task);
        taskInstance.setSubTaskNumber(ParserSparkHelper.getTaskID(input));
        session.save(taskInstance);

        TaskInstanceEvents eventLaunch = new TaskInstanceEvents();
        eventLaunch.setEventName("Launch");
        eventLaunch.setValueTimestamp(ParserSparkHelper.getLaunchTime(input));
        taskInstance.getTaskInstanceEventsSet().add(eventLaunch);
        eventLaunch.setTaskInstance(taskInstance);
        session.save(eventLaunch);

        if(firstEntry == null){
            firstEntry = eventLaunch.getValueTimestamp();
        }


        TaskInstanceEvents eventFinished = new TaskInstanceEvents();
        eventFinished.setEventName("Finished");
        eventFinished.setValueTimestamp(ParserSparkHelper.getFinishTime(input));
        taskInstance.getTaskInstanceEventsSet().add(eventFinished);
        eventFinished.setTaskInstance(taskInstance);
        session.save(eventFinished);

        if(lastEntry == null){
            lastEntry = eventFinished.getValueTimestamp();
        } else if(lastEntry.getTime() < eventFinished.getValueTimestamp().getTime()) {
            lastEntry = eventFinished.getValueTimestamp();
        }

        return taskInstance;
    }

    /**
     * returns the task of the given line. If the task is not created yet, it will create it.
     * @param input - a String line
     * @return Task of given line
     */
    private Task getTaskByLine(String input){
        String taskType = ParserSparkHelper.getTaskType(input);
        Task task = experimentRun.taskByTaskType(taskType);
        if(task == null){
            task = new Task();
            task.setExperimentRun(experimentRun);
            task.setTaskType(taskType);
            experimentRun.getTaskSet().add(task);
            session.save(task);
        }
        return task;
    }

    private void setSubmitTime(String line){
        JSONObject jsonObject = new JSONObject(line);
        experimentRun.setSubmitTime(new Date(jsonObject.getLong("Timestamp")));
    }
}
