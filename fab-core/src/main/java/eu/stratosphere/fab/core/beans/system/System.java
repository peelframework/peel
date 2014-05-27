package eu.stratosphere.fab.core.beans.system;

import java.util.Set;

abstract public class System {

    public enum Lifespan {
        SUITE,
        EXPERIMENT_SEQUENCE,
        EXPERIMENT,
        EXPERIMENT_RUN
    }

    public final Lifespan lifespan;

    public final Set<System> dependencies;

    public System(Lifespan lifespan, Set<System> dependencies) {
        this.lifespan = lifespan;
        this.dependencies = dependencies;
    }

    abstract public void setUp();

    abstract public void tearDown();
}
