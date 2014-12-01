DROP TABLE if exists ExperimentSuite;

Create Table ExperimentSuite
          (ExperimentSuiteID int not null,
          Name varchar(75),
          Primary Key (ExperimentSuiteID));

DROP TABLE if exists System;

Create Table System (
            ID int not null,
            Name varchar(50),
            Version varchar(75),
            Primary Key (ID));

DROP TABLE if exists Experiment;

Create Table Experiment (
            ID int not null,
            Name varchar(50) Not Null,
            SystemID int,
            ExperimentSuiteID int,
            RunsCount int,
            Primary Key (ID),
            Foreign Key (SystemID) References System(ID),
            Foreign Key (ExperimentSuiteID) References ExperimentSuite(ExperimentSuiteID));

DROP TABLE if exists ExperimentRun;

Create Table ExperimentRun (
            ID int not null,
            name varchar(35),
            ExperimentID int,
            Run int,
            submitTime Timestamp,
            deployed Timestamp,
            finished Timestamp,
            Primary Key (ID),
            Foreign Key (ExperimentID) references Experiment(ID));


DROP TABLE if exists Task;

Create Table Task (
            TaskID int not null,
            ExperimentRunID int,
            TaskType varchar(25),
            NumberOfSubtasks int,
            Primary Key(TaskID),
            Foreign Key (ExperimentRunID) References ExperimentRun(ID));

DROP TABLE if exists TaskInstance;

Create Table TaskInstance (
            TaskInstanceID int not null,
            TaskID int,
            SubtaskNumber int,
            Primary Key (TaskInstanceID),
            Foreign Key (TaskID) references Task(TaskID));

DROP TABLE if EXISTS TaskInstanceEvents;

CREATE TABLE TaskInstanceEvents (
			EventID int NOT NULL,
			TaskInstanceID int,
      EventName varchar(35),
			ValueInt int,
			ValueDouble DOUBLE,
			ValueTimestamp TIMESTAMP,
			ValueVarchar VARCHAR(65),
			PRIMARY KEY (EventID),
			FOREIGN Key(TaskInstanceID) REFERENCES TaskInstance(TaskInstanceID)
);

