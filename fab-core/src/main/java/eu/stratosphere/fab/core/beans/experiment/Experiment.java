package eu.stratosphere.fab.core.beans.experiment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.fab.core.beans.ExecutionContext;
import eu.stratosphere.fab.core.beans.system.ExperimentRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Experiment {

    static Logger logger = LoggerFactory.getLogger(Experiment.class);

    public final ExperimentRunner runner;

    public final Map<String, String> options;

    public final List<String> arguments;

    public Experiment(ExperimentRunner runner) {
        this.runner = runner;
        this.options = new HashMap<String, String>();
        this.arguments = new ArrayList<String>();
    }

    public Experiment(ExperimentRunner runner, Map<String, String> options) {
        this.runner = runner;
        this.options = options;
        this.arguments = new ArrayList<String>();
    }

    public Experiment(ExperimentRunner runner, List<String> arguments) {
        this.runner = runner;
        this.options = new HashMap<String, String>();
        this.arguments = arguments;
    }

    public Experiment(ExperimentRunner runner, Map<String, String> options, List<String> arguments) {
        this.runner = runner;
        this.options = options;
        this.arguments = arguments;
    }

    public void run(ExecutionContext context) {
        logger.info("Running experiment XXX");
        //this.runner.run(context);
    }
}
