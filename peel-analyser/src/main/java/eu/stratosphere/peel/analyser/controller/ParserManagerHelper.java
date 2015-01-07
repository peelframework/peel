package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.Experiment;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import eu.stratosphere.peel.analyser.model.ExperimentSuite;
import eu.stratosphere.peel.analyser.model.System;
import eu.stratosphere.peel.analyser.util.HibernateUtil;
import org.hibernate.Session;
import org.json.JSONObject;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Fabian on 08.11.14.
 */
class ParserManagerHelper {
    /**
     * gets the system saved in the database by the systemName
     * @param systemName the name of the system
     * @param version of the system
     * @return the system
     */
    protected static System getSystem(String systemName, String version){
        List<System> systemList;
        String query = "from System where name = :systemName and version = :version";
        HibernateUtil.getSession().beginTransaction();
        systemList = HibernateUtil.getSession()
                .createQuery(query)
                .setParameter("systemName", systemName)
                .setParameter("version", version)
                .list();
        HibernateUtil.getSession().getTransaction().commit();
        if(systemList.iterator().hasNext()) {
            return systemList.iterator().next();
        } else {
            return null;
        }
    }

    /**
     * saves a system as described by the state.json
     * @param stateJsonObj the state.json as a JSONObject
     * @return the created system
     */
    protected static System saveSystem(JSONObject stateJsonObj){
        Session session = HibernateUtil.getSession();
        session.beginTransaction();

        eu.stratosphere.peel.analyser.model.System system = new System();
        system.setName(stateJsonObj.getString("runnerName"));
        system.setVersion(stateJsonObj.getString("runnerVersion"));
        session.save(system);

        session.getTransaction().commit();
        return system;
    }

    /**
     * this method will search for the experimentSuite based on the name
     * @param experimentSuiteName the name of the experimentSuite
     * @return the experimentSuite or null if not found
     */
    protected static ExperimentSuite getExperimentSuite(String experimentSuiteName){
        String query = "from ExperimentSuite where name = :experimentSuiteName";
        HibernateUtil.getSession().beginTransaction();
        List<ExperimentSuite> experimentSuite = HibernateUtil.getSession().createQuery(query).setParameter("experimentSuiteName", experimentSuiteName).list();
        HibernateUtil.getSession().getTransaction().commit();
        if(experimentSuite.iterator().hasNext()){
            return experimentSuite.iterator().next();
        } else {
            return null;
        }
    }

    /**
     * if no experimentSuite was found this method will save the experimentSuite based on the
     * information of the state.json file
     * @param stateJsonObj The JSONObject of the state.json that describes the ExperimentRun
     * @return the experimentSuite that was created
     */
    protected static ExperimentSuite saveExperimentSuite(JSONObject stateJsonObj){
        Session session = HibernateUtil.getSession();
        session.beginTransaction();

        ExperimentSuite experimentSuite = new ExperimentSuite();
        experimentSuite.setName(stateJsonObj.getString("suiteName"));
        session.save(experimentSuite);

        session.getTransaction().commit();
        return experimentSuite;
    }

    /**
     * This method parses the JSONObject representing the state.json file and will search in the
     * database for the experiment of the experimentRun as described by the state.json.
     * @param stateJsonObj The JSONObject of the state.json that describes the ExperimentRun
     * @return the experiment of the experimentRun that was described by the state.json
     * @throws PeelAnalyserException
     */
    protected static Experiment getExperiment(JSONObject stateJsonObj) throws PeelAnalyserException {
        String query = "select experiment from Experiment as experiment join experiment.system as system join experiment.experimentSuite as experimentSuite where experiment.name = :experimentName AND system.name = :systemName AND experimentSuite.name = :experimentSuiteName";
        HibernateUtil.getSession().beginTransaction();
        List<Experiment> experiment = HibernateUtil.getSession().createQuery(query)
                .setParameter("experimentName", getExperimentName(stateJsonObj))
                .setParameter("systemName", stateJsonObj.get("runnerName"))
                .setParameter("experimentSuiteName", stateJsonObj.get("suiteName"))
                .list();
        HibernateUtil.getSession().getTransaction().commit();
        if(experiment.iterator().hasNext()){
            return experiment.iterator().next();
        } else {
            return null;
        }
    }

    /**
     * Will save a experiment based on the experimentName, the experimentSuite of this experiment
     * and the system this experiment was executed on.
     * @param experimentName The name of the experiment
     * @param suite The suite of the experiment
     * @param system The system this experiment was executed on
     * @return The created Experiment (is saved to database as well)
     */
    protected static Experiment saveExperiment(String experimentName, ExperimentSuite suite, System system){
        Session session = HibernateUtil.getSession();
        session.beginTransaction();

        Experiment experiment = new Experiment();
        experiment.setName(experimentName);
        experiment.setExperimentSuite(suite);
        suite.getExperimentSet().add(experiment);
        experiment.setSystem(system);
        system.getExperimentSet().add(experiment);
        session.save(experiment);
        session.update(system);
        session.update(suite);

        session.getTransaction().commit();
        return experiment;
    }

    /**
     * This method will parse the JSONObject representing the state.json file and will
     * return the run-number of the ExperimentRun that is described by the state.json
     * @param stateJsonObj
     * @return
     * @throws PeelAnalyserException
     */
    private static int parseExperimentRunCount(JSONObject stateJsonObj) throws PeelAnalyserException {
        Pattern pattern = Pattern.compile("(?<=.run)[0-9]+");
        Matcher matcher = pattern.matcher(stateJsonObj.getString("name"));
        if(matcher.find()) {
            String runCountString = matcher.group();
            return Integer.valueOf(runCountString);
        } else {
            throw new PeelAnalyserException("No match for ExperimentRunCount found");
        }
    }

    /**
     * This method will parse the JSONObject representing the state.json file
     * and will return the name of the ExperimentRun
     * @param stateJsonObj
     * @return
     * @throws PeelAnalyserException
     */
    protected static String getExperimentName(JSONObject stateJsonObj) throws PeelAnalyserException {
        Pattern pattern = Pattern.compile("[A-z0-9.]+(?=.run[0-9]+)");
        String input = stateJsonObj.getString("name");
        Matcher matcher = pattern.matcher(input);
        if(matcher.find()){
            return matcher.group();
        } else {
            throw new PeelAnalyserException("No match for ExperimentName found");
        }
    }

    /**
     * this method checks if the given filename is the jobmanager logfile
     * @param filename of the file you want to know if it's the jobmanager logfile
     * @return true if this is the jobmanager logfile, false if not
     */
    protected static boolean isJobmanager(String filename, String system){
        Pattern pattern = null;
        if(system.equals("flink")) {
            pattern = Pattern.compile("([A-z0-9-])+(-jobmanager-)([A-z0-9-])+(?=.log)");
        } else if(system.equals("spark")){
            pattern = Pattern.compile("EVENT_LOG_1");
        }
        Matcher matcher = pattern.matcher(filename);
        return matcher.find();
    }

    /**
     * this method finds the jobmanager file in the logs directory of a given ExperimentRun directory
     * @param experimentRunFile the directory of the experimentRun
     * @return the logfile of the jobmanager
     */
    protected static File findLogFile(File experimentRunFile, String system) {
        File[] files = experimentRunFile.listFiles();
        File logs = null;
        for (File file : files) {
            if (file.getName().equals("logs")) {
                logs = file;
            }
        }
        File[] logFiles = logs.listFiles();
        for (File logFile : logFiles) {
            if (isJobmanager(logFile.getName(), system)) {
                return logFile;
            }
        }
        return null;
    }

    /**
     * This method returns the System of a ExperimentRun
     * @param experimentRun
     * @return Systemname
     */
    protected static String getSystemOfExperimentRun(ExperimentRun experimentRun){
        System system = experimentRun.getExperiment().getSystem();
        return system.getName().toLowerCase();
    }

    /**
     * Saves a ExperimentRun into database based on the state.json file
     * @param experiment The experiment of the experimentRun
     * @param stateJsonObj The JSONObject of the state.json that describes the ExperimentRun
     * @return The created ExperimentRun
     * @throws PeelAnalyserException
     */
    protected static ExperimentRun saveExperimentRun(Experiment experiment, JSONObject stateJsonObj) throws PeelAnalyserException{
        Session session = HibernateUtil.getSession();
        session.beginTransaction();
        ExperimentRun experimentRun = new ExperimentRun();
        experimentRun.setExperiment(experiment);
        experimentRun.setRun(ParserManagerHelper.parseExperimentRunCount(stateJsonObj));
        experiment.getExperimentRunSet().add(experimentRun);
        session.save(experimentRun);
        session.update(experiment);
        session.getTransaction().commit();
        return experimentRun;
    }

    /**
     * checks if a ExperimentRun has failed.
     * @param stateJsonObj the state.json as a JSONObject
     * @return false if ExperimentRun finished correctly and true if ExperimentRun failed
     */
    protected static boolean isFailedExperimentRun(JSONObject stateJsonObj){
        return !stateJsonObj.get("runExitCode").equals(0);
    }

    /**
     * sets the AverageExperimentRunTime of every Experiment
     * ExperimentRunTime is defined as FinishTime - SubmitTime
     */
    protected static void setAverageExperimentRunTimes(){
        HibernateUtil.getSession().beginTransaction();
        List<Experiment> experimentList = HibernateUtil.getSession().createQuery("from Experiment").list();

        for(Experiment experiment: experimentList){
            long timeSum = 0;

            for(ExperimentRun experimentRun: experiment.getExperimentRunSet()){
                if(experimentRun.getFinished() != null && experimentRun.getSubmitTime() != null) {
                    timeSum += experimentRun.getFinished().getTime()
                                    - experimentRun.getSubmitTime().getTime();
                }
            }
            long timeAvg = timeSum / experiment.getExperimentRunSet().size();
            experiment.setAverageExperimentRunTime(timeAvg);
            HibernateUtil.getSession().update(experiment);
        }
        HibernateUtil.getSession().getTransaction().commit();
    }

}
