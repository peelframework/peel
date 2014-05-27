package eu.stratosphere.fab.core.beans.system;

import eu.stratosphere.fab.core.beans.ExecutionContext;

import java.util.Set;

abstract public class ExperimentRunner extends System {

    public ExperimentRunner(Lifespan lifespan, Set<System> dependencies) {
        super(lifespan, dependencies);
    }

    abstract public void run(ExecutionContext context);
}
