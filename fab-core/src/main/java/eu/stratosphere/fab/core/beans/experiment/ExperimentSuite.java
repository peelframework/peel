package eu.stratosphere.fab.core.beans.experiment;

import eu.stratosphere.fab.core.beans.ExecutionContext;

import java.util.List;

/**
 * Created by alexander on 27.05.14.
 */
public class ExperimentSuite {

    public final List<eu.stratosphere.fab.core.beans.experiment.Experiment> experiments;

    public ExperimentSuite(List<eu.stratosphere.fab.core.beans.experiment.Experiment> experiments) {
        this.experiments = experiments;
    }

    public void run() {
        // TODO: add logic here

        // 1.a) construct the systems graph (this can be done at the bottom of the constructor as well)
        // 1.b) create an execution context object referencing the systems dependency graph
        ExecutionContext context = new ExecutionContext();

        // run the experiments
        // 3.a) setup all systems with 'suite' lifespan in the execution context

        // 3.b) run each experiment in the experiments list
        for (Experiment e: experiments) {
            e.run(context);
        }

        // 3.c) tear down all systems with 'suite' lifespan in the execution context
    }
}
