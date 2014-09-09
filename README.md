peel
====

Peel is a framework for automatic large-scale experiments for massively-parallel systems and algorithms. 
It enables the conduction of fully automated and higly configurable experiments that can be executed from a simple command line interface.

### The main features are

- Specify and maintain a collection of reusable experiments
- Use default parameters or configure your systems with your favorite settings
- Run different experiments on a variety of systems
- Evaluate and compare the performance of your systems

#### Specify dependencies in your systems and let peel do the rest! 
Peel automatically sets up the systems that you specify while taking care of dependency-relations. If you want to change parameters between experiments in your suite, peel updates the corresponsing systems and their dependants automatically. All you need to do is specify your experiments and let peel do the rest!

*Supported Systems*
- Flink 0.5.1 & 0.6
- Hadoop 1.2.1 (HDFS and MapReduce)
- Spark 1.1.0


## Executing Experiments With Peel

The peel command line interface offers a comprehensive help of the available commands:

```bash
./peel -h
```

Please refer to the help screens for more detailed information.

### Executing a Single Experiment

If you are not sure if the configuration and the experiment jars work fine, you can run a single experiment with the following sequence of commands:

```bash
# 1. set-up all systems systems 
./peel exp:setup ${SUITE} ${EXPERIMENT} --fixtures ${FIXTURES}
# 2. just run the experiment, skip system set-up and tear-down
./peel exp:run ${SUITE} ${EXPERIMENT} --fixtures ${FIXTURES} --run 1 --just 
# 3. run the experiment without system set-up or tear-down
./peel exp:teardown ${SUITE} ${EXPERIMENT} --fixtures ${FIXTURES}
```

Halt and repeat the second step if your algorithm does not terminate or is too slow. When you are done, make sure you execute the third step to shut everything down.

### Executing All Experiments in a Suite 

To run a you use the **suite:run** command and specify the fixtures file and the id of the suite in your fixture:

```bash
./peel suite:run --fixtures ${FIXTURES} ${SUITE}
```

Results from the suite experiments are written into the **${app.path.results}/${suite}/${experiment.name}** folders. 
The **state.json** file in each folder contains information about the exit state of the experiment. 
Per default, running the same suite again will skip experiments that returned a zero exit code.
If you want to rerun a particular experiment, delete the corresponding experiment folder before you rerun the suite.

## Creating Fixtures


In **fixtures.wordcount.xml** you can see a minimal example that runs the example Stratosphere wordcount job with one, two, three, and four slaves. 
The following table provides a short descriptions of the beans configured in this file.

Bean                       | Description
:--------------------------|------------------------
stratosphere               | Defines stratosphere as the system to use with the dependency to hdfs
dataset.shakespeare        | The data set used for the experiment. The src arg specifies the location of the compressed data set. The dst arg specifies the location the data set is extracted to.
experiment.stratosphere    | Abstract specification for an experiment. Defines the runs (6) and the system used (stratosphere)
experiment.stratosphre.wc  | Specialization of experiment.stratosphere. Specifies the executed command (<job jar> <input> <output>) and the data set used.
wc.local / wc.cloud-7      | Specifies a suite (collection) of experiments to be executed as a whole.

To setup your own experiment for your algorithm, you just have to create a fixture describing the experiment.
The **fixtures.template.xml** contains preconfigured beans for all data sets as well as inline pointers for adapting the file to your algorithm.
You can use this file as a starting point for your configuration. Overall, the required steps are:

1. Introduce a new suffix for your experiment.
1. Replace the command string with the right path to your jar, input and output file.
1. Replace the referenced data set with the one suitable for your experiment.

Placeholders in the fixture files (e.g. ${app.path.datasets}) are references to values specified in configuration files. 
For each experiment, Peel will load and resolve the following hierarchy of configuration files:

1. Reference application configuration: [**resource:reference.conf**](https://github.com/citlab/peel/blob/master/peel-core/src/main/resources/reference.conf). 
1. For each **system** with bean id **systemID**: 
   1. Reference system configuration [**reference.${system}.conf**](https://github.com/citlab/peel/blob/master/peel-extensions/src/main/resources).
   1. Custom bean configuration located at **${app.path.config}/${systemID}.conf** (optional).
   1. Host-specific custom bean configuration located at **${app.path.config}/${hostname}/${systemID}.conf** (optional).
1. Custom application configuration located at **${app.path.config}/application.conf** (optional).
1. Custom application configuration located at **${app.path.config}/${hostname}/application.conf** (optional).
1. Experiment configuration defined in the **config** argument of the current experiment.
1. The Java system properties.

### Example: WordCount

This section contains some examples based on the example **wordcount** fixtures file.

To execute **run #1** from the **wc.single-run** experiment in the **wc.default** suite, type:

```bash
./peel exp:setup wc.default wc.single-run --fixtures ./config/fixtures.wordcount.xml
./peel exp:run wc.default wc.single-run --fixtures ./config/fixtures.wordcount.xml --run 1 --just 
./peel exp:teardown wc.default wc.single-run --fixtures ./config/fixtures.wordcount.xml
```

To execute all runs runs from all experiments in the suite, type:

```bash
./peel run-suite --fixtures ./config/fixtures.wordcount.xml <suit-name>
```
