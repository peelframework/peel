package eu.stratosphere.peel.analyser.model;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Representing a Experiment. A Experiment consists of many ExperimentRuns.
 * After parsing the field averageExperimentRunTime is set by calculating the
 * average runtime of all experimentRuns relating to this experiment
 * Created by Fabian on 15.10.14.
 */

@Entity
public class Experiment {
    private Integer ExperimentID;
    private System system;
    private String name;
    private int runs;
    private Set<ExperimentRun> experimentRunSet;
    private ExperimentSuite experimentSuite;
    private long averageExperimentRunTime;

    public Experiment() {
        experimentRunSet = new HashSet<>();
    }

    @ManyToOne
    @JoinColumn
    public ExperimentSuite getExperimentSuite() {
        return experimentSuite;
    }

    public void setExperimentSuite(ExperimentSuite experimentSuite) {
        this.experimentSuite = experimentSuite;
    }

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn
    public Set<ExperimentRun> getExperimentRunSet() {
        return experimentRunSet;
    }

    public void setExperimentRunSet(Set<ExperimentRun> experimentRunSet) {
        this.experimentRunSet = experimentRunSet;
    }

    @Id
    @GeneratedValue
    public Integer getExperimentID() {
        return ExperimentID;
    }

    public void setExperimentID(Integer ID) {
        this.ExperimentID = ID;
    }

    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn
    public System getSystem() {
        return system;
    }

    public void setSystem(System system) {
        this.system = system;
    }

    @Column
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Column
    public int getRuns() {
        return runs;
    }

    public void setRuns(int runs) {
        this.runs = runs;
    }

    @Column
    public long getAverageExperimentRunTime() {
        return averageExperimentRunTime;
    }

    public void setAverageExperimentRunTime(long averageExperimentRunTime) {
        this.averageExperimentRunTime = averageExperimentRunTime;
    }
}
