package eu.stratosphere.peel.analyser.model;

import org.hibernate.Session;

import javax.persistence.*;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by ubuntu on 15.10.14.
 */
@Entity
public class TaskInstance
{
    private Integer TaskInstanceID;
    private Integer subTaskNumber;
    private Set<TaskInstanceEvents> taskInstanceEventsSet;
    private Task task;

    public TaskInstance() {
        taskInstanceEventsSet = new HashSet<>();
    }

    /*getter and setter */

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn
    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    @Column
    public Integer getSubTaskNumber() {
        return subTaskNumber;
    }

    public void setSubTaskNumber(Integer subTaskNumber) {
        this.subTaskNumber = subTaskNumber;
    }

    @Id
    @GeneratedValue
    public Integer getTaskInstanceID() {
        return TaskInstanceID;
    }

    public void setTaskInstanceID(Integer ID) {
        this.TaskInstanceID = ID;
    }

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn
    public Set<TaskInstanceEvents> getTaskInstanceEventsSet() {
        return taskInstanceEventsSet;
    }

    public void setTaskInstanceEventsSet(Set<TaskInstanceEvents> taskInstanceEventsSet) {
        this.taskInstanceEventsSet = taskInstanceEventsSet;
    }

    /**
     * gets the time in milliseconds which took the task from creation to execution
     * @return OverheadTime in milliseconds
     */
    /*
    public long getManagementOverhead() throws PeelAnalyserException {
        try {
            return starting.getTime() - created.getTime();
        } catch(NullPointerException e){
            throw new PeelAnalyserException("Subtask Exception in getManagementOverhead - one of starting or created is null");
        }
    }

    /**
     * gets the time in milliseconds which took the task from starting to finishing
     * @return the Calculation Time in milliseconds
     */
    /*
    public long getCalculationTime() throws PeelAnalyserException {
        try {
            return finishingFinished.getTime() - starting.getTime();
        } catch(NullPointerException e){
            throw new PeelAnalyserException("Subtask NullPointerException in getCalculationTime - one of finishingFinished or starting is null");
        }
    }
    @Override
    public boolean equals(Object o){
        if(!(o instanceof TaskInstance)){
            return false;
        }
        if(!((TaskInstance) o).getTaskInstanceID().equals(this.getTaskInstanceID())){
            return false;
        }
        return true;
    }
    /**
     * this method will change the statusChange attribute based on the "statusChange" parameter. It can handle both
     * the versions "statusA to statusB" as well as "statusAstatusB" (e.g. "SCHEDULED to STARTING" as well as "SCHEDULEDSTARTING")
     * @param statusChange
     * @param timestamp
     */

    public void addTimeStampToStatusChange(String statusChange, Date timestamp, Session session){
        TaskInstanceEvents taskInstanceEvents = new TaskInstanceEvents();
        taskInstanceEvents.setTaskInstance(this);
        taskInstanceEvents.setEventName(statusChange.toLowerCase());
        taskInstanceEvents.setValueTimestamp(timestamp);
        this.taskInstanceEventsSet.add(taskInstanceEvents);
        session.save(taskInstanceEvents);
    }

    /**
     * returns the event specified by this name.
     * @param name the eventName
     * @return the corresponding taskInstanceEvents
     */
    public TaskInstanceEvents getEventByName(String name){
        Iterator<TaskInstanceEvents> events = taskInstanceEventsSet.iterator();
        TaskInstanceEvents event;
        while(events.hasNext()){
            event = events.next();
            if(event.getEventName().toLowerCase().equals(name.toLowerCase())){
                return event;
            }
        }
        return null;
    }
}
