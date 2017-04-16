---
layout: manual
title: Bundle Basics
date: 2015-07-05 10:00:00
nav: [ manual, bundle-basics]
---

# Bundle Basics

A Peel bundle packages together the configuration data, datasets, and programs required for the execution of a particular set of experiments.

## Folder Structure

The top-level elements of a Peel bundle are shortly summarized below.

| Default Path    | Config Parameter     | Fixed | Description                                               |
| --------------- | -------------------- |:-----:| --------------------------------------------------------- |
| ./apps          | `app.path.apps`      | Yes   | Workload applications.                                    |
| ./config        | `app.path.config`    | Yes   | Environment configurations and experiments definitions.   |
| ./datagens      | `app.path.datagens`  | No    | Data generators.                                          |
| ./datasets      | `app.path.datasets`  | No    | Static datasets.                                          |
| ./downloads     | `app.path.downloads` | No    | Archived system binaries.                                 |
| ./lib           | `app.path.log`       | Yes   | Peel libraries and dependencies.                          |
| ./log           | `app.path.log`       | Yes   | Peel execution logs.                                      |
| ./results       | `app.path.results`   | No    | State and log data from experiment runs.                  |
| ./systems       | `app.path.systems`   | No    | Contains all running systems.                             |
| ./utils         | `app.path.utils`     | No    | Utility scripts and files.                                |
| ./peel.sh       | `app.path.cli`       | Yes   | The Peel command line interface.                          |

You can customize the [default paths](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/resources/reference.peel.conf) of non-fixed paths in your bundle- or host-specific configuration. For more information how to do this, please refer to the [Environment Configurations]({{ site.baseurl }}/manual/environment-configurations.html) section.

## Components

The entries in the above list can be principally grouped as follows.

### Peel CLI and Logs

The Peel command line tool. Invoke it without arguments to see the list of supported commands. While running, the Peel CLI spawns and executes OS processes. In the log folder, you will find the `stdout` and `stderr` output of these processes, as well as a copy of the actual console output produced by Peel itself.

### Configurations and Definitions

The *config* folder contains *\*.conf* files written in [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) syntax which define the environment configuration, as well as *\*.xml* files with the actual experiments defined as [Spring beans](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/xsd-config.html).

### Workload Applications

The *apps* folder contains the binaries of the experiment workload applications.

### Data Sets and Generators

The *datasets* folder contains static, fixed-sized datasets required for the experiments. The *datagens* folder contains programs for dynamic generation of scalable datasets required for the experiments.

### Systems

The *downloads* folder contains system binary archives for the systems in your experiment environment managed by Peel. The system archives are per default extracted in the systems folder.

### Results

The *results* folder contains data collected from attempted successful Peel experiment runs in a hierarchy following a *${suite}/${expName}.run${NN}* naming convention.

### Utils

The *utils* folder contains utility scripts (e.g., SQL queries and [gnuplot](http://www.gnuplot.info/) scripts) that can be used next to or in conjunction with Peel CLI commands.

## Example

Taking a closer look at the *peel-wordcount* bundle from the [Motivation]({{ site.baseurl }}/manual/motivation.html#solution) section, we can see the following project and folder structure.

### Bundle Sources

The bundle sources are organized as a multi-module Maven project with the following structure:

```bash
# cd "$BUNDLE_SRC" && \
# tree -L 1 --dirsfirst -d peel-wordcount
peel-wordcount                     # parent module
├── peel-wordcount-bundle          # module for bundle assembly
├── peel-wordcount-datagens        # module for data generators
├── peel-wordcount-flink-jobs      # module for Flink jobs 
├── peel-wordcount-peelextensions  # module for Peel extensions
└── peel-wordcount-spark-jobs      # module for Spark jobs
```

### Assembled Bundle 

The assembled bundle for the `peel-wordcount` bundle looks as follows:

```bash
# cd "$BUNDLE_BIN" && \
# tree -L 2 --dirsfirst peel-wordcount
peel-wordcount
├── apps
│   ├── peel-wordcount-flink-jobs-1.0-SNAPSHOT.jar
│   └── peel-wordcount-spark-jobs-1.0-SNAPSHOT.jar
├── config
├── datagens
│   └── peel-wordcount-datagens-1.0-SNAPSHOT.jar
├── datasets
├── downloads
├── lib
│   ├── ...
│   ├── peel-core-1.0-SNAPSHOT.jar
│   ├── peel-extensions-1.0-SNAPSHOT.jar
│   ├── peel-wordcount-peelextensions-1.0-SNAPSHOT.jar
│   └── ...
├── log
├── results
├── systems
├── utils
├── peel.sh
└── VERSION
```

Here is how the Maven modules from the bundle sources are mapped to the assembled bundle.

* The *apps* folder contains the jar artifacts for all `*-jobs` and `*-apps` modules. 
* The *datagens* folder contains the jar artifacts for all `*-datagens` modules. 
* The *lib* folder contains the two common Peel artifacts `peel-core` and `peel-extensions`, as well as the `*-peelextensions` artifact where the user can put hers bundle-specific Peel extensions.
* The contents of the remaining folders are copied from the corresponding folder located under `peel-wordcount-bundle/src/main/resources`.

### Building

To assemble the bundle from the sources with Maven, you have one of four options:

```bash
mvn clean package       # package in peel-wordcount-bundle
mvn clean package -Pdev # package with soft links
mvn clean deploy        # package and copy to $BUNDLE_BIN
mvn clean deploy -Pdev  # package and copy with soft links
```

Running the `deploy` phase automatically copies the assembled bundle binaries folder to `$BUNDLE_BIN`.

Activating the `dev` profile (with `-Pdev`) enforces certain folders in the assembled bundle binaries (*config*, *datasets*, and  *utils*) to be created as soft links to their corresponding source locations.
