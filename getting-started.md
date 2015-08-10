---
layout: simple
title: Getting Started
nav: getting-started
---

# Getting Started

A Peel package bundles together the configuration data, datasets, and programs required for the execution of a particular set of experiments, and is therefore shortly referred to as a *Peel bundle*. 

To get started, you need to get a Peel bundle using one of two methods -- a *Pre-Packaged Binary* or a *Maven Archetype*. 

The snippets of code below rely on the following shell variables. Modify them accordingly to reflect your developer machine environment.

{% highlight bash %}
export BUNDLE_BIN=/path/to/bundle/binaries          # bundle binaries
export BUNDLE_SRC=/path/to/bundle/sources           # bundle sources
export BUNDLE_GID=com.acme.peel                     # bundle group
export BUNDLE_AID=peel-bundle                       # bundle name
export BUNDLE_PKG=com.acme.benchmarks.example       # bundle package
{% endhighlight %}

If you intend to maintain multiple bundles, we suggest to keep `BUNDLE_BIN` and `BUNDLE_SRC` in your `~/.profile` file.

## Pre-Packaged Binary

If you don't want to version the code and configuration data of your bundle, your best option is to download and extract the [pre-packaged empty bundle archive](http://peel-framework.org/peel-empty-bundle.tar.gz).

{% highlight bash %}
wget http://peel-framework.org/peel-bundle.tar.gz   # download
tar -xzvf peel-bundle.tar.gz -C "$BUNDLE_BIN"       # extract
mv "$BUNDLE_BIN/peel-bundle"                        \
   "$BUNDLE_BIN/$BUNDLE_AID"                        # rename
cd "$BUNDLE_BIN/$BUNDLE_AID"                        # go to bin
{% endhighlight %}

## Maven Archetype

If you intend to version the code and configuration data in your bundle, the best way to start is to use a Peel archetype.

{% highlight bash %}
cd "$BUNDLE_SRC"                                    # go to src
mvn archetype:generate -B                           \
    -DarchetypeGroupId=org.peelframework            \
    -DarchetypeArtifactId=peel-flinkspark-bundle    \
    -DarchetypeVersion=1.0-SNAPSHOT                 \
    -DgroupId=$BUNDLE_GID                           \
    -DartifactId=$BUNDLE_AID                        \
    -Dpackage=$BUNDLE_PKG                           # init. bundle
cd "$BUNDLE_AID"                                    # go to bundle
mvn clean deploy -DskipTests                        # build & deploy
cd "$BUNDLE_BIN/$BUNDLE_AID"                        # go to bin
{% endhighlight %}

The following archetypes are currently supported:

| Archetype ID                 | Description                                                       |
| ---------------------------- | ----------------------------------------------------------------- |
| `peel-flinkspark-bundle`     | A bundle for versioned workload applications for Spark & Flink.   |
| `peel-flink-bundle`          | A bundle for versioned workload applications for Flink only.      |
| `peel-spark-bundle`          | A bundle for versioned workload applications for Spark only.      |

## Run the Example Experiment

From the `$BUNDLE_BIN/$BUNDLE_AID` directory, run the following command:

{% highlight bash %}
./peel.sh suite:run wordcount.default
{% endhighlight %}

This will execute an example Wordcount job on Flink and Spark on your local machines.

## Check the Results

To check the results of the example job, run

{% highlight bash %}
./peel.sh db:initialize                  # init. results database
./peel.sh db:import wordcount.default    # import experiment results
{% endhighlight %}

If you're using the archetype method, you can also execute a custom query on the results:

{% highlight bash %}
./peel.sh res:import wordcount.default    # import experiment results
{% endhighlight %}

## Next Steps

Interested in learning more? Check the [Motivation]({{ site.baseurl }}/manual/motivation.html) section for a brief introduction to system experiments and an overview of the problems Peel will solve for you.
Alternatively, go directly to [Bundle Basics]({{ site.baseurl }}/manual/bundle-basics.html) if you want to get your hands right away!