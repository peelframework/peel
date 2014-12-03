package eu.stratosphere.peel.analyser.controller;


import java.io.File;

/**
 * Created by Fabian on 10.11.2014.
 */
public class Main {

    private static final String usageNotification = "Usage: ./peelanalyser --parse pathToEntryFolder [--skipInstances]";

    public static void main(String[] args) throws Exception{
        if(args.length != 2 || args.length != 3){
            System.out.println(usageNotification);
        }

        boolean skipInstances = false;
        if(args.length == 3 && args[2].equals("--skipInstances")) {
            skipInstances = true;
        }

        File entryFolder = new File(args[1]);
        if(!entryFolder.isDirectory()){
            System.out.println(usageNotification);
        }

        ParserManager parserManager = new ParserManager(args[1], skipInstances);

        try {
            parserManager.parsePath();
        } catch (Exception e){
            System.err.println(e.getMessage());
            throw e;
        }
    }
}
