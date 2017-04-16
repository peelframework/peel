---
layout: simple
title: Getting Started
nav: getting-started
---

# Getting Started

A Peel package bundles together the configuration data, datasets, and workload applications required for the execution of a particular collection of experiments, and is therefore shortly referred to as a *Peel bundle*. 

To get started, you need to bootstrap a Peel bundle using one of two methods -- a [*Pre-Packaged Binary*](#pre-packaged-binary) or a [*Maven Archetype*](#maven-archetype). 

The code snippets below assume that the following shell variables are set. Modify them accordingly before running the code.

```bash
# for all bundles
export BUNDLE_BIN=~/bundles/bin                          # bundle binaries parent
export BUNDLE_SRC=~/bundles/src                          # bundle sources parent
# for the current bundle
export BUNDLE_GID=com.acme                               # bundle groupId
export BUNDLE_AID=peel-wordcount                         # bundle artifactId
export BUNDLE_PKG=com.acme.benchmarks.wordcount          # bundle root package
```

*__Tip__: If you intend to maintain multiple bundles, we suggest to define `BUNDLE_BIN` and `BUNDLE_SRC` in your `~/.profile` file.*

## Pre-Packaged Binary

If you don't want to version the code and configuration data of your bundle, your best option is to download and extract the [pre-packaged empty bundle archive](http://peel-framework.org/peel-empty-bundle-{{ site.current_version }}.tar.gz).

```bash
wget https://github.com/stratosphere/peel/releases/download/v{{ site.current_version }}/peel-empty-bundle-{{ site.current_version }}.tar.gz
mkdir -p "$BUNDLE_BIN/$BUNDLE_AID"
tar -xzvf peel-empty-bundle-{{ site.current_version }}.tar.gz -C "$BUNDLE_BIN/$BUNDLE_AID"
cd "$BUNDLE_BIN/$BUNDLE_AID"
```

## Maven Archetype

If you intend to version the code and configuration data of your bundle, your best option is to bootstrap a project structure from a Peel archetype.

```bash
cd "$BUNDLE_SRC"
mvn archetype:generate -B                         \
    -Dpackage="$BUNDLE_PKG"                       \
    -DgroupId="$BUNDLE_GID"                       \
    -DartifactId="$BUNDLE_AID"                    \
    -DarchetypeGroupId=org.peelframework          \
    -DarchetypeArtifactId=peel-flinkspark-bundle  \
    -DarchetypeVersion={{ site.current_version }}
cd "$BUNDLE_AID"
mvn clean deploy
cd "$BUNDLE_BIN/$BUNDLE_AID"
```

The following archetypes are currently supported:

| Archetype ID                 | Description                                                       |
| ---------------------------- | ----------------------------------------------------------------- |
| `peel-flinkspark-bundle`     | A bundle with versioned workload applications for Spark & Flink.  |
| `peel-flink-bundle`          | A bundle with versioned workload applications for Flink only.     |
| `peel-spark-bundle`          | A bundle with versioned workload applications for Spark only.     |

## Run the Example Experiment

From the `$BUNDLE_BIN/$BUNDLE_AID` directory, run the following command:

```bash
./peel.sh suite:run wordcount.default
```

This will trigger the execution of an example suite which consists of two experiments running a Wordcount job on Flink and Spark respectively. 
Each job will be repeated three times, and the results and raw log data for each run will be stored in the `results/wordcount.default` folder.

## Check the Results

Peel ships with facilities to extract, transform, and load the row data from your experiments in a relational database.
To do this for the Wordcount job, run the following commands:

```bash
./peel.sh db:initialize
./peel.sh db:import wordcount.default
```

You can then start analyzing your experiment data with SQL queries and data analysis tools that can use a relational database as a backend. 
For example, the following SQL query retrieves the min, median, and max runtime for the two experiments in the `wordcount.default` suite.

```sql
SELECT   e.suite                                 as suite       ,
         e.name                                  as name        ,
         MIN(r.time)                             as min_time    ,
         MAX(r.time)                             as max_time    ,
         SUM(r.time) - MIN(r.time) - MAX(r.time) as median_time
FROM     experiment                              as e           ,
         experiment_run                          as r
WHERE    e.id    = r.experiment_id
AND      e.suite = "wordcount.default"
GROUP BY e.suite, e.name
ORDER BY e.suite, e.name
```

If you're using the archetype method, you can run the above query directly through the custom Peel command shipped with your bundle:

```bash
./peel.sh query:runtimes wordcount.default
```

## Next Steps

Interested in learning more? 
Check the [Motivation]({{ site.baseurl }}/manual/motivation.html) section for a brief introduction to system experiment vocabulary and concepts and an overview of the problems Peel will solve for you.
Alternatively, go directly to [Bundle Basics]({{ site.baseurl }}/manual/bundle-basics.html) if you want to get your hands dirty right away!
