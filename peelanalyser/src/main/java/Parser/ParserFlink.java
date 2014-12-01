package Parser;

import Exception.PeelAnalyserException;
import Model.ExperimentRun;
import Model.Task;
import Model.TaskInstance;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ubuntu on 18.10.14.
 */
public class ParserFlink implements Parser {

    private ExperimentRun experimentRun;

    private static final Pattern patternTaskType = Pattern.compile("(DataSink)|(Reduce)|(CHAIN)|(PartialSolution)|(Map)|(Combine)");
    private Session session = null;
    boolean skipInstances;

    public ParserFlink(boolean skipInstances) {
        this.skipInstances = skipInstances;
    }

    public ParserFlink(ExperimentRun experimentRun, Session session) {
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
        String line = null;
        Transaction transaction = session.beginTransaction();
        while((line = in.readLine()) != null){
            if(ParserFlinkHelper.isJob(line)){
                handleJobInput(line);
                session.update(experimentRun);          //if not called the experimentRun will not be updated, since it was saved in another transaction. All other Objects are stored after all fields had been parsed
            } else if(!skipInstances){
                handleTaskInstanceInput(line);
                session.update(experimentRun);
            }
        }
        transaction.commit();
    }

    /**
     * this method handles a logfile line which specifies a job. It will add the statusChangeTime
     * @param jobInput
     */
    private void handleJobInput(String jobInput) throws PeelAnalyserException{
        if(ParserFlinkHelper.isReleasingInstance(jobInput)) { return;}       //not useful information
        if(ParserFlinkHelper.isRequestSlotsEntry(jobInput)) { return; }      //not useful information either
        if(ParserFlinkHelper.isSubmitJob(jobInput)){
            experimentRun.setSubmitTime(ParserFlinkHelper.getTimeStamp(jobInput));
            return;
        }
        if(ParserFlinkHelper.isDeploydJob(jobInput)){
            experimentRun.setDeployed(ParserFlinkHelper.getTimeStamp(jobInput));
            return;
        }
        if(ParserFlinkHelper.isFinishedJob(jobInput)){
            experimentRun.setFinished(ParserFlinkHelper.getTimeStamp(jobInput));
            return;
        }
    }

    /**
     * this method handles a logfile line which specifies a subtask. If the subtask is not yet created it will create it
     * and add it to the subtask list of the <i>task</i>. If the subtask is created it will alter the statusChangeTime
     * and add the one which is added in this line
     * @param taskInstanceInput (a line of the logfile with the subtask)
     */
    private void handleTaskInstanceInput(String taskInstanceInput) throws PeelAnalyserException{
        String taskTypeString = "";
        Task task = null;

        Matcher matcherTaskType = patternTaskType.matcher(taskInstanceInput);
        if(matcherTaskType.find()){
            taskTypeString = matcherTaskType.group();
        } else {
            throw new PeelAnalyserException("ParserFlink handleTaskInstanceInput - could not find the tasktype in this job");
        }

        //if there is not task with this tasktype then create one
        if((task = experimentRun.taskByTaskType(taskTypeString)) == null){
            task = new Task();
            task.setTaskType(taskTypeString);
            task.setExperimentRun(experimentRun);
            experimentRun.getTaskSet().add(task);
            session.save(task);
        }

        handleTaskInstance(task, taskInstanceInput);


    }

    private void handleTaskInstance(Task task, String taskInstanceInput) throws PeelAnalyserException{
        String taskType = "";
        TaskInstance taskInstance = null;

        int subTaskNumber = ParserFlinkHelper.getSubTaskNumber(taskInstanceInput);
        Date timestampDate = ParserFlinkHelper.getTimeStamp(taskInstanceInput);
        String statusChange = ParserFlinkHelper.getStatusChange(taskInstanceInput);

        if((taskInstance = task.taskInstanceBySubtaskNumber(subTaskNumber)) == null){
            taskInstance = new TaskInstance();
            taskInstance.setSubTaskNumber(subTaskNumber);
            taskInstance.setTask(task);
            task.getTaskInstances().add(taskInstance);
            session.save(taskInstance);
            taskInstance.addTimeStampToStatusChange(statusChange, timestampDate, session);
            session.update(taskInstance);
        } else {
            taskInstance.addTimeStampToStatusChange(statusChange, timestampDate, session);
            session.update(taskInstance);
        }

    }



}
