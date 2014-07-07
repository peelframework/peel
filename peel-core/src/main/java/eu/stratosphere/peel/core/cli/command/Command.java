package eu.stratosphere.peel.core.cli.command;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

abstract public class Command {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Returns a constant name for this command.
     * 
     * @return The unique name for this command
     */
    abstract public String name();

    /**
     * Returns a constant one-sentence help for this command.
     * 
     * @return The help message for this command
     */
    abstract public String help();

    /**
     * Registers the arguments and options to the CLI sub-parser for this command.
     */
    abstract public void register(Subparser parser);

    /**
     * Configures the selected command using the parsed CLI arguments.
     * 
     * @param ns The parsed CLI arguments namespace.
     */
    abstract public void configure(Namespace ns);

    /**
     * Runs the command.
     * 
     * @param context The application context
     */
    abstract public void run(ApplicationContext context);
}
