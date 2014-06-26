package eu.stratosphere.peel.core;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Paths;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import eu.stratosphere.peel.core.beans.experiment.ExperimentSuite;

public class Application {

    private final Logger logger = LoggerFactory.getLogger(Application.class);

    void run() {
        try {
            logger.info("############################################################");
            logger.info("#              PEEL TEST EXECUTION FRAMEWORK               #");
            logger.info("############################################################");
            logger.info(String.format("Running experiments from %s", System.getProperty("app.path.fixtures")));

            String[] c = new String[] {"classpath:peel-core.xml", "classpath:peel-extensions.xml", "file:" + System.getProperty("app.path.fixtures")};
            AbstractApplicationContext context = new FileSystemXmlApplicationContext(c);
            context.registerShutdownHook();

            ExperimentSuite experimentSuite = context.getBean(System.getProperty("app.suite"), ExperimentSuite.class);
            experimentSuite.run();
        } catch (Exception e) {
            logger.error("Error in experiment suite: " + e.getMessage());
            System.exit(-1);
        }
    }

    @SuppressWarnings("resource")
    public static void main(String... args) throws Exception {
        ArgumentParser parser = getArgumentParser();

        try {
            Namespace ns = parser.parseArgs(args);

            // set ns options and arguments to system properties
            System.setProperty("app.path.config", Paths.get(ns.getString("app.path.config")).normalize().toAbsolutePath().toString());
            System.setProperty("app.path.log", Paths.get(ns.getString("app.path.log")).normalize().toAbsolutePath().toString());
            System.setProperty("app.path.fixtures", Paths.get(ns.getString("app.path.fixtures")).normalize().toAbsolutePath().toString());
            System.setProperty("app.suite", ns.getString("app.suite"));
            System.setProperty("app.hostname", ns.getString("app.hostname"));

            // add new root file appender
            org.apache.log4j.RollingFileAppender appender = new org.apache.log4j.RollingFileAppender();
            appender.setLayout(new org.apache.log4j.PatternLayout("%5p [%-8t] (%-38c{1}) - %m%n"));
            appender.setFile(String.format("%s/application.log", System.getProperty("app.path.log")), true, true, 4096);
            appender.setMaxFileSize("100KB");
            appender.setMaxBackupIndex(1);
            org.apache.log4j.Logger.getRootLogger().addAppender(appender);

            // start and run the application
            new Application().run();
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
        }
    }

    private static ArgumentParser getArgumentParser() {
        //@formatter:off
        ArgumentParser parser = ArgumentParsers.newArgumentParser("peel")
                .defaultHelp(true)
                .description("System experiments toolkit.");
        Subparsers subparsers = parser.addSubparsers()
                .help("Run an experiment suite.");

        // general options
        parser.addArgument("--config")
                .type(String.class)
                .dest("app.path.config")
                .setDefault("config")
                .metavar("PATH")
                .help("config folder");
        parser.addArgument("--log")
                .type(String.class)
                .dest("app.path.log")
                .setDefault("log")
                .metavar("PATH")
                .help("log folder");
        parser.addArgument("--hostname")
                .type(String.class)
                .dest("app.hostname")
                .setDefault(getHostname())
                .metavar("NAME")
                .help("this machine hostname");

        // command: run-suite
        Subparser runSuite = subparsers.addParser("run-suite", true);
        runSuite.addArgument("--fixtures")
                .type(String.class)
                .dest("app.path.fixtures")
                .setDefault("config/fixtures.xml")
                .metavar("FIXTURES")
                .help("fixtures file");
        runSuite.addArgument("suite")
                .type(String.class)
                .dest("app.suite")
                .metavar("SUITE")
                .help("experiments suite to run");
        //@formatter:on

        return parser;
    }

    private static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (IOException e) {
            return "localhost";
        }
    }
}
