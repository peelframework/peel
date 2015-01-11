package eu.stratosphere.peel.analyser.model;

import org.hibernate.Session;

import javax.persistence.*;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A TaskInstance is a instance of a task that was executed in the experimentRun.
 * A TaskInstance can have various events that may depend on the system the experiment was
 * executed on.
 * Created by Fabian on 15.10.14.
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

    @OneToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinColumn
    public Set<TaskInstanceEvents> getTaskInstanceEventsSet() {
        return taskInstanceEventsSet;
    }

    public void setTaskInstanceEventsSet(Set<TaskInstanceEvents> taskInstanceEventsSet) {
        this.taskInstanceEventsSet = taskInstanceEventsSet;
    }

    /**
     * this method will change the statusChange attribute based on the "statusChange" parameter. It can handle both
     * the versions "statusA to statusB" as well as "statusAstatusB" (e.g. "SCHEDULED to STARTING" as well as "SCHEDULEDSTARTING")
     * @param statusChange
     * @param timestamp
     */

    public TaskInstanceEvents addTimeStampToStatusChange(String statusChange, Date timestamp){
        TaskInstanceEvents taskInstanceEvents = new TaskInstanceEvents();
        taskInstanceEvents.setTaskInstance(this);
        taskInstanceEvents.setEventName(statusChange.toLowerCase());
        taskInstanceEvents.setValueTimestamp(timestamp);
        this.taskInstanceEventsSet.add(taskInstanceEvents);
        return taskInstanceEvents;
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
