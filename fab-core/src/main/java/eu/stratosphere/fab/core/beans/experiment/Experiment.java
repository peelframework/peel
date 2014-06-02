package eu.stratosphere.fab.core.beans.experiment;

import java.util.*;

import eu.stratosphere.fab.core.beans.ExecutionContext;
import eu.stratosphere.fab.core.beans.system.ExperimentRunner;
import eu.stratosphere.fab.core.beans.system.JavaExperimentRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Experiment {

    static Logger logger = LoggerFactory.getLogger(Experiment.class);

    public final String name;

    public final ExperimentRunner runner;

    public final Map<String, String> options;

    public final List<String> arguments;

    public Experiment(ExperimentRunner runner) {
        this.name = "Experiment " + UUID.randomUUID().toString();
        this.runner = runner;
        this.options = new HashMap<String, String>();
        this.arguments = new ArrayList<String>();
    }

    public Experiment(String name, ExperimentRunner runner) {
        this.name = name;
        this.runner = runner;
        this.options = new HashMap<String, String>();
        this.arguments = new ArrayList<String>();
    }

    public Experiment(String name, ExperimentRunner runner, Map<String, String> options) {
        this.name = name;
        this.runner = runner;
        this.options = options;
        this.arguments = new ArrayList<String>();
    }

    public Experiment(String name, ExperimentRunner runner, List<String> arguments) {
        this.name = name;
        this.runner = runner;
        this.options = new HashMap<String, String>();
        this.arguments = arguments;
    }

    public Experiment(String name, ExperimentRunner runner, Map<String, String> options, List<String> arguments) {
        this.name = name;
        this.runner = runner;
        this.options = options;
        this.arguments = arguments;
    }

    public void run(ExecutionContext context) {
        logger.info("Running experiment XXX with runner " + runner.name);
        //this.runner.run(context);
    }
}
