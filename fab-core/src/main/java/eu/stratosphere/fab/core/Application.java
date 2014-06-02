package eu.stratosphere.fab.core;

import eu.stratosphere.fab.core.beans.experiment.ExperimentSuite;
import eu.stratosphere.fab.core.beans.experiment.JavaExperimentSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Alexander Alexandrov <alexander.alexandrov@tu-berlin.de>
 */
public class Application {

    static Logger logger = LoggerFactory.getLogger(Application.class);

    @SuppressWarnings("resource")
    public static void main(String... args) throws Exception {

        String experimentsConfigPath = args.length >= 1 ? args[0] : "fab-experiments.xml";

        logger.info("FAB Application");
        AbstractApplicationContext context = new ClassPathXmlApplicationContext("fab-core.xml", "fab-extensions.xml", experimentsConfigPath);
        context.registerShutdownHook();

        ExperimentSuite experimentSuite = context.getBean("experiments", ExperimentSuite.class);
        experimentSuite.run();
    }
}
