package Model;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by ubuntu on 15.10.14.
 */
@Entity
public class Task {
    private Integer TaskID;
    private Integer numberOfSubtasks;
    private String taskType;
    private Set<TaskInstance> taskInstances;
    private ExperimentRun experimentRun;


    public Task() {
        numberOfSubtasks = 0;
        taskInstances = new HashSet<TaskInstance>();
    }

    /* getter and setter*/

    @Id
    @GeneratedValue
    public Integer getTaskID() {
        return TaskID;
    }

    public void setTaskID(Integer ID) {
        this.TaskID = ID;
    }

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn
    public ExperimentRun getExperimentRun() {
        return experimentRun;
    }

    public void setExperimentRun(ExperimentRun experimentRun) {
        this.experimentRun = experimentRun;
    }

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn
    public Set<TaskInstance> getTaskInstances() {
        return taskInstances;
    }

    public void setTaskInstances(Set<TaskInstance> taskInstances) {
        this.taskInstances = taskInstances;
    }

    @Column
    public Integer getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public void setNumberOfSubtasks(Integer numberOfSubtasks) {
        this.numberOfSubtasks = numberOfSubtasks;
    }


    @Column
    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    /**
     * gets the time which takes the task from starting to running.
     * the task cannot start before its subtasks are created and will switch to running before one of its subtasks is running.
     * @return Management Overhead in milliseconds
     */
    /*public long getManagementOverhead(){
        return startingTime.getTime() - creationTime.getTime();
    }
    */
    /**
     * gets the time which takes the task for execution.
     * the task will start before one of its subtasks are running and will finish after all of them finished.
     * @return Calculation Time in milliseconds
     */
    /*public long getCalculationTime(){
        return endTime.getTime() - startingTime.getTime();

    }*/

    public TaskInstance taskInstanceBySubtaskNumber(int subtaskNumber){
        Iterator<TaskInstance> taskInstanceIterator = taskInstances.iterator();
        TaskInstance taskInstance = null;
        while (taskInstanceIterator.hasNext()){
            taskInstance = taskInstanceIterator.next();
            if(taskInstance.getSubTaskNumber() == subtaskNumber){
                return taskInstance;
            }
        }
        return null;
    }
}
