package Controller;

import Model.*;
import Model.System;
import Exception.PeelAnalyserException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by ubuntu on 20.10.14.
 */
public class DatabaseManager {
    public static Connection connection = null;
    public static String databasePath = "jdbc:h2:./src/main/resources/DatabasePeelAnalyser";

    public static void initialiseDatabase() throws PeelAnalyserException {
        try {
            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection(databasePath);
        } catch (ClassNotFoundException e) {
            throw new PeelAnalyserException("DatabaseManager - cannot find the jdbc connector 'h2'. Please add it to the build path.");
        } catch (SQLException e) {
            throw new PeelAnalyserException("DatabaseManager - cannot make the connection to database in path: " + databasePath);
        }

    }
    /*
    public static void createTables() throws PeelAnalyserException {
        executeCreates(ExperimentSuite.createExperimentSuite);
        executeCreates(System.createSystem);
        executeCreates(TaskType.createTaskType);
        executeCreates(Experiment.createExperiment);
        executeCreates(ExperimentRun.createExperimentRun);
        executeCreates(Task.createTask);
        executeCreates(TaskInstance.createTaskInstance);

    }
    */
    public static void executeCreates(String createStatement) throws PeelAnalyserException {
        try {
            Statement statement = connection.createStatement();
            statement.execute(createStatement);
        } catch (SQLException e) {
            throw new PeelAnalyserException("Database - cannot execute the Statement '" + createStatement + "'. Please make sure you have called initialiseDatabase() first. [Message] " + e.getMessage());
        }
    }

   public static void closeConnection() throws PeelAnalyserException {
       try {
           connection.close();
       } catch (SQLException e) {
           throw new PeelAnalyserException("Database - cannot close connection (was it ever initialised?)");
       }
   }

   public static void deleteTables() throws PeelAnalyserException {
       try {
           if(connection.isClosed()){
               initialiseDatabase();
           }
           Statement statement = connection.createStatement();
           String[] drops = {"Test", "Experiment", "ExperimentRun", "ExperimentSuite", "System", "Task", "TaskInstance", "TaskType"};
           for(int i = 0; i<drops.length; i++){
               try {
                   statement.execute("DROP TABLE " + drops[i]);
               } catch (Exception e){
               }
           }
       } catch (SQLException e) {
           //throw new PeelAnalyserException("DatabaseManager - cannot drop database [Message:]" + e.getMessage());
       }
   }
}
