package eu.stratosphere.peel.analyser.parser;

import java.io.BufferedReader;
import java.io.IOException;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import org.hibernate.Session;

/**
 * Created by ubuntu on 08.11.14.
 */
public interface Parser {
    public void parse(BufferedReader in) throws IOException, PeelAnalyserException;

    public ExperimentRun getExperimentRun();

    public void setExperimentRun(ExperimentRun experimentRun);
}
