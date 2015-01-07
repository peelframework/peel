package eu.stratosphere.peel.analyser.model;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Set;

/**
 * A ExperimentSuite is a Suite of various Experiments
 * that can be executed on different systems.
 * Created by Fabian on 15.10.14.
 */
@Entity
public class ExperimentSuite {
    private Integer ExperimentSuiteID;
    private Set<Experiment> experimentSet;
    private String name;

    public ExperimentSuite() {
        experimentSet = new HashSet<>();
    }

    @Column
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn
    public Set<Experiment> getExperimentSet() {
        return experimentSet;
    }

    public void setExperimentSet(Set<Experiment> experimentSet) {
        this.experimentSet = experimentSet;
    }

    @Id
    @GeneratedValue
    public Integer getExperimentSuiteID() {
        return ExperimentSuiteID;
    }

    public void setExperimentSuiteID(Integer ID) {
        this.ExperimentSuiteID = ID;
    }
}
