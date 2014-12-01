package Parser;

import java.io.BufferedReader;
import java.io.IOException;

import Exception.PeelAnalyserException;
import Model.ExperimentRun;
import org.hibernate.Session;

/**
 * Created by ubuntu on 08.11.14.
 */
public interface Parser {
    public void parse(BufferedReader in) throws IOException, PeelAnalyserException;

    public ExperimentRun getExperimentRun();
    public Session getSession();
    public void setSession(Session session);
    public void setExperimentRun(ExperimentRun experimentRun);
}
