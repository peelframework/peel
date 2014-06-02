package eu.stratosphere.fab.core.beans.system;

import eu.stratosphere.fab.core.beans.ExecutionContext;
import java.util.Set;

abstract public class JavaExperimentRunner extends System {

    public JavaExperimentRunner(String name, Lifespan lifespan, Set<System> dependencies) {
        super(name, lifespan, dependencies);
    }

    abstract public void run(ExecutionContext context);
}
