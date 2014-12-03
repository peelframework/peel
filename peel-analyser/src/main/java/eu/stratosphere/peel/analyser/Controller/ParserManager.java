package eu.stratosphere.peel.analyser.Controller;

import eu.stratosphere.peel.analyser.Exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.Model.Experiment;
import eu.stratosphere.peel.analyser.Model.ExperimentRun;
import eu.stratosphere.peel.analyser.Model.ExperimentSuite;
import eu.stratosphere.peel.analyser.Model.System;
import eu.stratosphere.peel.analyser.Parser.Parser;
import eu.stratosphere.peel.analyser.Parser.ParserFlink;
import eu.stratosphere.peel.analyser.Parser.ParserSpark;
import eu.stratosphere.peel.analyser.Util.ExperimentRunFile;
import eu.stratosphere.peel.analyser.Util.HibernateUtil;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created by ubuntu on 08.11.14.
 */
public class ParserManager {

    LinkedList<ExperimentRunFile> experimentRunFileList = new LinkedList<ExperimentRunFile>();
    File rootPath;
    Parser parser = null;
    boolean skipInstances;

    public ParserManager(String path, boolean skipInstances){
        rootPath = new File(path);
        this.skipInstances = skipInstances;
    }

    /**
     * this method will read the entire File Structure produced by Peel and will pass each ExperimentRun - Log to the Flink- or Spark Parser
     * @throws PeelAnalyserException
     * @throws java.io.IOException
     */
    public void parsePath() throws Exception {
        searchExperimentRuns();

        for(int i = 0; i < experimentRunFileList.size(); i++){

            //get experimentRun and system

            ExperimentRun experimentRun = experimentRunFileList.get(i).getExperimentRun();
            String system = ParserManagerHelper.getSystemOfExperimentRun(experimentRun);

            File logFile = ParserManagerHelper.findLogFile(experimentRunFileList.get(i).getFile(), system);
            if(logFile == null){
                throw new PeelAnalyserException("Parser Manager - could not find a LogFile");
            }
            BufferedReader reader = new BufferedReader(new FileReader(logFile));

            //create parser
            if(system.equals("flink")){
                parser = new ParserFlink(skipInstances);
            } else if(system.equals("spark")){
                parser = new ParserSpark(skipInstances);
            } else {
                throw new PeelAnalyserException("ParserManager - could not identify the system of experimentRun. ExpRun: " + experimentRun.getExperiment().getName() + experimentRun.getRun() + " System: " + system);
            }

            //setup Parser
            parser.setSession(HibernateUtil.getSession());
            if(experimentRun != null) {
                parser.setExperimentRun(experimentRun);
            } else {
                throw new PeelAnalyserException("ParserManager - ExperimentRun is null");
            }
            java.lang.System.out.println("ParserManager - ExperimentenRun started [name:] " + experimentRun.getExperiment().getName() + ".run" + experimentRun.getRun());
            parser.parse(reader);
            reader.close();
        }
        ParserManagerHelper.setAverageExperimentRunTimes();
    }

    /**
     * this method will search for ExperimentRuns starting from the root Directory
     * @throws PeelAnalyserException
     */
    public void searchExperimentRuns() throws PeelAnalyserException{
        if(!rootPath.isDirectory()) throw new PeelAnalyserException("Der angegebene Pfad ist kein Verzeichnis. Bitte geben Sie ein Verzeichnis an.");

        File[] experimentDirectory = rootPath.listFiles();
        for(int i = 0; i < experimentDirectory.length; i++){
            File[] experimentRunDirectories = experimentDirectory[i].listFiles();
            for(int j = 0; j < experimentRunDirectories.length; j++){
                ExperimentRun experimentRun = getExperimentRun(experimentRunDirectories[j]);
                if(experimentRun != null) {
                    experimentRunFileList.add(new ExperimentRunFile(experimentRun, experimentRunDirectories[j]));
                }
            }
        }
    }

    /**
     * this method will get the ExperimentRun object from the given ExperimentRun - Directory
     * @param experimentRunDirectory
     * @return ExperimentRun Object
     */
    public ExperimentRun getExperimentRun(File experimentRunDirectory) throws PeelAnalyserException{
        File[] experimentRunFiles = experimentRunDirectory.listFiles();
        File stateJson = null;
        for(int i = 0; i < experimentRunFiles.length; i++) {
            if (experimentRunFiles[i].getName().equals("state.json")) {
                stateJson = experimentRunFiles[i];
            }
        }
        ExperimentRun result = null;
        try {
            result = parseStateJson(stateJson);
        } catch (Exception e){
            throw new PeelAnalyserException(e.getMessage());
        }
        return result;
    }

    /**
     * this method will parse a state.json file and will return the specific ExperimentRun
     * @param stateJson
     * @return
     */
    public ExperimentRun parseStateJson(File stateJson) throws IOException, PeelAnalyserException{
        BufferedReader stateJsonReader = new BufferedReader(new FileReader(stateJson));
        StringBuilder stringBuilder = new StringBuilder();
        String line = "";
        while ((line = stateJsonReader.readLine()) != null){
            stringBuilder.append(line);
        }
        JSONObject stateJsonObj = new JSONObject(stringBuilder.toString());

        if(ParserManagerHelper.isFailedExperimentRun(stateJsonObj)){
            return null;
        }

        System system;
        if((system = ParserManagerHelper.getSystem(stateJsonObj.getString("runnerName"))) == null){
            system = ParserManagerHelper.saveSystem(stateJsonObj);
        }

        ExperimentSuite experimentSuite;
        if((experimentSuite = ParserManagerHelper.getExperimentSuite(stateJsonObj.getString("suiteName"))) == null){
            experimentSuite = ParserManagerHelper.saveExperimentSuite(stateJsonObj);
        }

        Experiment experiment;
        String experimentName = ParserManagerHelper.getExperimentName(stateJsonObj);
        if((experiment = ParserManagerHelper.getExperiment(stateJsonObj)) == null){
            experiment = ParserManagerHelper.saveExperiment(experimentName, experimentSuite, system);
        }

        return ParserManagerHelper.saveExperimentRun(experiment, stateJsonObj);

    }
}
