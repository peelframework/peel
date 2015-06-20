package eu.stratosphere.peel.analyser.model;

import javax.persistence.*;
import java.util.*;

/**
 * A ExperimentRun is a specific run of a Experiment. The field run tells which one of the
 * various runs of the experiment is this specific run.
 * A ExperimentRun consists of many Tasks.
 *
 * Created by Fabian on 18.10.14.
 */
@Entity
public class ExperimentRun {
    private Integer ExperimentRunID;
    private int run;
    private Date submitTime;
    private Date deployed;
    private Date finished;
    private Set<Task> taskSet;
    private Experiment experiment;

    public ExperimentRun() {
        taskSet = new HashSet<>();
    }

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn
    public Experiment getExperiment() {
        return experiment;
    }

    public void setExperiment(Experiment experiment) {
        this.experiment = experiment;
    }

    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn
    public Set<Task> getTaskSet() {
        return taskSet;
    }

    public void setTaskSet(Set<Task> taskSet) {
        this.taskSet = taskSet;
    }

    @Id
    @GeneratedValue
    public Integer getExperimentRunID() {
        return ExperimentRunID;
    }

    public void setExperimentRunID(Integer ID) {
        this.ExperimentRunID = ID;
    }

    @Column
    public int getRun() {
        return run;
    }

    public void setRun(int run) {
        this.run = run;
    }

    @Column(columnDefinition = "TIMESTAMP")
    public Date getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Date submitTime) {
        this.submitTime = submitTime;
    }

    @Column(columnDefinition = "TIMESTAMP")
    public Date getDeployed() {
        return deployed;
    }

    public void setDeployed(Date deployed) {
        this.deployed = deployed;
    }

    @Column(columnDefinition = "TIMESTAMP")
    public Date getFinished() {
        return finished;
    }

    public void setFinished(Date finished) {
        this.finished = finished;
    }

    public long returnManagementOverhead(){
        return deployed.getTime() - submitTime.getTime();
    }

    public long returnCalculationTime(){
        return finished.getTime() - deployed.getTime();
    }

    /*
     * gets the Task by the taskType
     * @param taskType
     * @return Task
     */
    public Task taskByTaskType(String taskType){
        Iterator<Task> taskIterator = taskSet.iterator();
        Task task;
        while(taskIterator.hasNext()){
            task = taskIterator.next();
            if(task.getTaskType() == null){
                return null;
            }
            if (task.getTaskType().equals(taskType)){
                return task;
            }
        }
        return null;
    }

}
