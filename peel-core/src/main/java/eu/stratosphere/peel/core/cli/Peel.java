package eu.stratosphere.peel.core.cli;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ServiceLoader;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.internal.HelpScreenException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import eu.stratosphere.peel.core.cli.command.Command;

public class Peel {

    @SuppressWarnings("resource")
    public static void main(String... args) throws Exception {

        // empty args calls help
        if (args.length == 0) {
            args = new String[] { "--help" };
        }

        // load available commands
        HashMap<String, Command> commands = new HashMap<>();
        for (Command command : ServiceLoader.load(Command.class)) {
            try {
                // put command in map
                commands.put(command.name(), command);
            } catch (Throwable e) {
                System.out.println(String.format("ERROR: %s", e.getMessage()));
                System.exit(1);
            }
        }

        // construct base argument parser
        ArgumentParser parser = getArgumentParser();

        // register command arguments with the arguments parser (in order of command names)
        String[] commandKeys = commands.keySet().toArray(new String[commands.size()]);
        Arrays.sort(commandKeys);
        for (String key : commandKeys) {
            Command c = commands.get(key);
            c.register(parser.addSubparsers().addParser(c.name(), true).help(c.help()));
        }

        try {
            // parse the arguments
            Namespace ns = parser.parseArgs(args);

            // register general options as system properties
            System.setProperty("app.command", ns.getString("app.command"));
            System.setProperty("app.hostname", ns.getString("app.hostname"));
            System.setProperty("app.path.config", Paths.get(ns.getString("app.path.config")).normalize().toAbsolutePath().toString());
            System.setProperty("app.path.log", Paths.get(ns.getString("app.path.log")).normalize().toAbsolutePath().toString());

            // add new root file appender
            org.apache.log4j.RollingFileAppender appender = new org.apache.log4j.RollingFileAppender();
            appender.setLayout(new org.apache.log4j.PatternLayout("%d{yy-MM-dd HH:mm:ss} [%p] %m%n"));
            appender.setFile(String.format("%s/peel.log", System.getProperty("app.path.log")), true, true, 4096);
            appender.setMaxFileSize("100KB");
            appender.setMaxBackupIndex(1);
            org.apache.log4j.Logger.getRootLogger().addAppender(appender);

            // make sure that command exists
            if (!commands.containsKey(System.getProperty("app.command"))) {
                throw new RuntimeException(String.format("Unexpected command '%s'", System.getProperty("app.command")));
            }

            // execute command workflow:
            // 1) print application header
            final Logger logger = LoggerFactory.getLogger(Peel.class);
            printHeader(logger);

            // 2) construct and configure command
            Command command = commands.get(System.getProperty("app.command"));
            command.configure(ns);

            // 3) construct application context
            //@formatter:off
            AbstractApplicationContext context = new ClassPathXmlApplicationContext((null != System.getProperty("app.path.experiments"))
                    ? new String[] {"classpath:peel-core.xml", "classpath:peel-extensions.xml", "file:" + System.getProperty("app.path.experiments")}
                    : new String[] {"classpath:peel-core.xml", "classpath:peel-extensions.xml"});
            context.registerShutdownHook();
            //@formatter:on

            // 4) execute command and return
            command.run(context);
        } catch (HelpScreenException e) {
            parser.handleError(e);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(-1);
        } catch (Throwable e) {
            System.err.println(String.format("Unexpected error: %s", e.getMessage()));
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }

    private static ArgumentParser getArgumentParser() {
        //@formatter:off
        ArgumentParser parser = ArgumentParsers.newArgumentParser("peel")
                .defaultHelp(true)
                .description("A toolkit for execution of system experiments.");
        parser.addSubparsers()
                .help("a command to run")
                .dest("app.command")
                .metavar("COMMAND");

        // general options
        parser.addArgument("--hostname")
                .type(String.class)
                .dest("app.hostname")
                .setDefault(getHostname())
                .metavar("NAME")
                .help("hostname for config resolution");
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

    private static void printHeader(Logger logger) {
        logger.info("############################################################");
        logger.info("#              PEEL TEST EXECUTION FRAMEWORK               #");
        logger.info("############################################################");
    }
}
