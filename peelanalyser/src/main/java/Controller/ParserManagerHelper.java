package Controller;

import Exception.PeelAnalyserException;
import Model.Experiment;
import Model.ExperimentRun;
import Model.ExperimentSuite;
import Model.System;
import Util.HibernateUtil;
import org.hibernate.Session;
import org.json.JSONObject;

import java.io.File;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ubuntu on 08.11.14.
 */
public class ParserManagerHelper {
    /**
     * gets the system saved in the database by the systemName
     * @param systemName
     * @return
     */
    public static System getSystem(String systemName){
        List<System> systemList;
        String query = "from System where name = :systemName";
        HibernateUtil.getSession().beginTransaction();
        systemList = HibernateUtil.getSession()
                .createQuery(query).setParameter("systemName", systemName).list();
        HibernateUtil.getSession().getTransaction().commit();
        if(systemList.iterator().hasNext()) {
            return systemList.iterator().next();
        } else {
            return null;
        }
    }

    public static System saveSystem(JSONObject stateJsonObj){
        Session session = HibernateUtil.getSession();
        session.beginTransaction();

        Model.System system = new System();
        system.setName(stateJsonObj.getString("runnerName"));
        system.setVersion(stateJsonObj.getString("runnerVersion"));
        session.save(system);

        session.getTransaction().commit();
        return system;
    }

    public static ExperimentSuite getExperimentSuite(String experimentSuiteName){
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

    public static ExperimentSuite saveExperimentSuite(JSONObject stateJsonObj){
        Session session = HibernateUtil.getSession();
        session.beginTransaction();

        ExperimentSuite experimentSuite = new ExperimentSuite();
        experimentSuite.setName(stateJsonObj.getString("suiteName"));
        session.save(experimentSuite);

        session.getTransaction().commit();
        return experimentSuite;
    }

    public static Experiment getExperiment(JSONObject stateJsonObj) throws PeelAnalyserException {
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

    public static Experiment saveExperiment(String experimentName, ExperimentSuite suite, System system){
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

    public static int parseExperimentRunCount(JSONObject stateJsonObj) throws PeelAnalyserException {
        Pattern pattern = Pattern.compile("(?<=.run)[0-9]+");
        Matcher matcher = pattern.matcher(stateJsonObj.getString("name"));
        if(matcher.find()) {
            String runCountString = matcher.group();
            return Integer.valueOf(runCountString);
        } else {
            throw new PeelAnalyserException("No match for ExperimentRunCount found");
        }
    }

    public static String getExperimentName(JSONObject stateJsonObj) throws PeelAnalyserException {
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
     * @param filename
     * @return
     */
    public static boolean isJobmanager(String filename, String system){
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
     * @param experimentRunFile
     * @return
     * @throws Exception
     */
    public static File findLogFile(File experimentRunFile, String system) throws Exception {
        File[] files = experimentRunFile.listFiles();
        File logs = null;
        for(int i = 0; i<files.length; i++){
            if(files[i].getName().equals("logs")){
                logs = files[i];
            }
        }
        File[] logFiles = logs.listFiles();
        for(int i = 0; i<logFiles.length; i++){
            if(isJobmanager(logFiles[i].getName(), system)){
                return logFiles[i];
            }
        }
        return null;
    }

    public static String getSystemOfExperimentRun(ExperimentRun experimentRun){
        System system = experimentRun.getExperiment().getSystem();
        return system.getName().toLowerCase();
    }

    public static ExperimentRun saveExperimentRun(Experiment experiment, JSONObject stateJsonObj) throws PeelAnalyserException{
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
     * @param stateJsonObj
     * @return false if ExperimentRun finished correctly and true if ExperimentRun failed
     */
    public static boolean isFailedExperimentRun(JSONObject stateJsonObj){
        return !stateJsonObj.get("runExitCode").equals(0);
    }

    /**
     * sets the AverageExperimentRunTime of every Experiment
     */
    public static void setAverageExperimentRunTimes(){
        HibernateUtil.getSession().beginTransaction();
        List<Experiment> experimentList = HibernateUtil.getSession().createQuery("from Experiment").list();

        for(Experiment experiment: experimentList){
            long timeSum = 0;

            for(ExperimentRun experimentRun: experiment.getExperimentRunSet()){
                timeSum += experimentRun.getFinished().getTime() -  experimentRun.getSubmitTime().getTime();
            }
            long timeAvg = timeSum / experiment.getExperimentRunSet().size();
            experiment.setAverageExperimentRunTime(timeAvg);
            HibernateUtil.getSession().update(experiment);
        }
        HibernateUtil.getSession().getTransaction().commit();
    }

}
