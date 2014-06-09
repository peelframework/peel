package eu.stratosphere.fab.core.beans.system;

import java.util.Set;

abstract public class JavaSystem {

    public enum JavaLifespan {
        SUITE,
        EXPERIMENT_SEQUENCE,
        EXPERIMENT,
        EXPERIMENT_RUN
    }

    public final String name;
    public final JavaLifespan lifespan;
    public final Set<System> dependencies;

    public JavaSystem(String name, JavaLifespan lifespan, Set<System> dependencies) {
        this.name = name;
        this.lifespan = lifespan;
        this.dependencies = dependencies;
    }

    abstract public void setUp();

    abstract public void tearDown();
}
