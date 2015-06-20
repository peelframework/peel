package eu.stratosphere.peel.analyser.controller;

import eu.stratosphere.peel.analyser.exception.PeelAnalyserException;
import eu.stratosphere.peel.analyser.model.Experiment;
import eu.stratosphere.peel.analyser.model.ExperimentRun;
import eu.stratosphere.peel.analyser.model.ExperimentSuite;
import eu.stratosphere.peel.analyser.model.System;
import eu.stratosphere.peel.analyser.parser.Parser;
import eu.stratosphere.peel.analyser.parser.ParserFlink;
import eu.stratosphere.peel.analyser.parser.ParserSpark;
import eu.stratosphere.peel.analyser.util.ExperimentRunFile;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public class ParserManager {

    private final LinkedList<ExperimentRunFile> experimentRunFileList = new LinkedList<>();
    private final File rootPath;
    private Parser parser = null;
    private final boolean skipInstances;

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

        for (ExperimentRunFile anExperimentRunFileList : experimentRunFileList) {

            //get experimentRun and system

            ExperimentRun experimentRun = anExperimentRunFileList.getExperimentRun();
            String system = ParserManagerHelper.getSystemOfExperimentRun(experimentRun);

            File logFile = ParserManagerHelper.findLogFile(anExperimentRunFileList.getFile(), system);
            if (logFile == null) {
                throw new PeelAnalyserException("Parser Manager - could not find a LogFile");
            }
            BufferedReader reader = new BufferedReader(new FileReader(logFile));

            //create parser
            switch (system) {
                case "flink":
                    parser = new ParserFlink(skipInstances);
                    break;
                case "spark":
                    parser = new ParserSpark(skipInstances);
                    break;
                default:
                    throw new PeelAnalyserException("ParserManager - could not identify the system of experimentRun. ExpRun: " + experimentRun.getExperiment().getName() + experimentRun.getRun() + " System: " + system);
            }

            //setup Parser
            if (experimentRun != null) {
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
    void searchExperimentRuns() throws PeelAnalyserException{
        if(!rootPath.isDirectory()) throw new PeelAnalyserException("The path is no directory. Please enter a valid directory.");

        File[] experimentDirectory = rootPath.listFiles();
        for (File anExperimentDirectory : experimentDirectory) {
            File[] experimentRunDirectories = anExperimentDirectory.listFiles();
            for (File experimentRunDirectory : experimentRunDirectories) {
                ExperimentRun experimentRun = getExperimentRun(experimentRunDirectory);
                if (experimentRun != null) {
                    experimentRunFileList.add(new ExperimentRunFile(experimentRun, experimentRunDirectory));
                }
            }
        }
    }

    /**
     * this method will get the ExperimentRun object from the given ExperimentRun - Directory
     * @param experimentRunDirectory the directory of the experimentRun
     * @return ExperimentRun Object
     */
    ExperimentRun getExperimentRun(File experimentRunDirectory) throws PeelAnalyserException{
        File[] experimentRunFiles = experimentRunDirectory.listFiles();
        File stateJson = null;
        for (File experimentRunFile : experimentRunFiles) {
            if (experimentRunFile.getName().equals("state.json")) {
                stateJson = experimentRunFile;
            }
        }
        ExperimentRun result;
        try {
            result = parseStateJson(stateJson);
        } catch (Exception e){
            throw new PeelAnalyserException(e.getMessage());
        }
        return result;
    }

    /**
     * this method will parse a state.json file and will return the specific ExperimentRun
     * @param stateJson the state.json file from a experimentRun
     * @return the experimentRun
     */
    ExperimentRun parseStateJson(File stateJson) throws IOException, PeelAnalyserException{
        BufferedReader stateJsonReader = new BufferedReader(new FileReader(stateJson));
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = stateJsonReader.readLine()) != null){
            stringBuilder.append(line);
        }
        JSONObject stateJsonObj = new JSONObject(stringBuilder.toString());

        if(ParserManagerHelper.isFailedExperimentRun(stateJsonObj)){
            return null;
        }

        System system;
        if((system = ParserManagerHelper.getSystem(stateJsonObj.getString("runnerName"), stateJsonObj.getString("runnerVersion"))) == null){
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
