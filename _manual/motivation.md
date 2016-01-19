---
layout: manual
title: Motivation
date: 2015-07-04 10:00:00
nav: [ manual, motivation ]
---

# {{ page.title }}

This section describes the domain of system experiments, introduces a running example used throughout the rest of the manual, and discusses the problems Peel will solve for you.

## System Experiments Basics

The principle goal of a system experiment is to characterise the behavior of a particular *system under test (SUT)* for a specific set of values configured as *system and application parameters*. The usual way to achieve this goal is to 

1. define a *workload application* which takes specific parameters (e.g., input and output path),
2. run it on top of the SUT with a specific configuration (e.g., allocated memory, <acronym title="degree of parallelism">DOP</acronym>), and 
3. measure key performance characteristics (e.g., runtime, throughput, accuracy). 

The basic layout of the execution environment observed when running a system experiment is depicted below.

<div class="row">
    <figure class="large-7 large-centered medium-8 medium-centered small-10 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_basic_simple.svg" title="Basic experiment environment layout" alt="Basic experiment environment layout" /><br />
        <figcaption>Basic experiment environment layout</figcaption>
    </figure>
</div>

Modern data management, however, rarely relies on a single system running in isolation. Insead, current trends advocate an architecture based on interconnected systems (e.g.[ HDFS and Yarn](http://hadoop.apache.org),[ Spark](http://spark.apache.org/),[ Flink](http://flink.apache.org/),[ Storm](https://storm.apache.org/)), each maintaining its own configuration. A more realistic environment layout therefore looks as follows:

<div class="row">
    <figure class="large-7 large-centered medium-8 medium-centered small-10 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_basic.svg" title="Experiment environment layout with multiple systems" alt="Experiment environment layout with multiple systems" /><br />
        <figcaption>Experiment environment layout with multiple systems</figcaption>
    </figure>
</div>

Typically, one is not just interested in the insight obtained by a single experiment, but in trends highlighted by a suite of experiments where a certain SUT configuration or application parameter value is varied and everything else remains fixed.

<div class="row">
    <figure class="large-9 large-centered medium-10 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_basic_series.svg" title="A suite of experiments with varying SUT config" alt="A suite of experiments with varying SUT config" /><br />
        <figcaption>A suite of experiments with varying SUT config</figcaption>
    </figure>
</div>

## Running Example

To illustrate the principles presented above, let us consider a scenario where we want to compare the scale-out characteristics of two parallel dataflow engines - [Apache Spark](http://spark.apache.org/) and [Apache Flink](http://flink.apache.org/) - based on their ability to count words. 
The environment layout for a single run of the two applications looks like this:

<div class="row">
    <figure class="large-7 large-centered medium-8 medium-centered small-10 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc.svg" title="Environment layout for 'WordCount: Spark vs. Flink'" alt="Environment layout for 'WordCount: Spark vs. Flink'" /><br />
        <figcaption>Environment layout for "WordCount: Spark vs. Flink"</figcaption>
    </figure>
</div>

### Step 1: Defining a Workload Application

We start by coding two equivalent workload applications - *SparkWC* and *FlinkWC* - against the corresponding parallel dataflow APIs of the two systems. The applications

1. read a text corpus from HDFS, 
2. compute the word frequencies within that corpus, and 
3. store the resulting collection of (word, count) back in HDFS. 

### Step 2: Defining the Input Data

In addition, we also write a parallel data generator that generates a random sequence of words to be used as input. To make the properties of the data realistic, the words are sampled i.i.d. out of a pre-defined dictionary following a Zipfian distribution.

### Step 3: Running the Experiments

We are now ready execute a series experiments on a cluster with varying capacity. In each step, we perform the following actions:

1. Setup the required systems, while doubling the number of DataNodes (HDFS) / TaskManagers (Flink) / Workers (Spark);
2. Generate and store input data in HDFS, while keeping the size/nodes ratio fixed;
3. Repeatedly execute the two applications and record key performance metrics for each experiment run.

The environment layout will look as follows:

<div class="row">
    <figure class="large-9 large-centered medium-10 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc_series.svg" title="Environment layouts for 'Weak Scale-Out - Spark vs. Flink'" alt="Environment layouts for 'Weak Scale-Out - Spark vs. Flink'" /><br />
        <figcaption>Environment layouts for "Weak Scale-Out - Spark vs. Flink"</figcaption>
    </figure>
</div>

### Step 4: Result Analysis / Closing the Loop

The obtained data characterizes the so-called [weak-scaling](https://en.wikipedia.org/wiki/Scalability#Weak_versus_strong_scaling) behavior of the two systems for the word counting scenario. Upon running the experiments, the most common steps are:

1. Understand the experiment results (e.g., via visual exploration, statistical analysis);
2. Make hypotheses that explain the observed phenomenon;
3. Refine the old experiment or create a new one in order to verify them.

## Challenges

If you, as a practitioner, want to realize the steps outlined above, you will face the following challenges.

### Environment Configuration

Each system comes with its own installation and configuration manual. You need to have good prior knowledge or invest time reading these manuals in order to understand (i) which parameters work out of the box, (ii) which ones need to be tuned to your current execution environment, and (iii) which what values to chose for those.

### Execution Code

When you come to [Step 3](#step-3-running-the-experiments), you will have the option to either sit in front of the console and steer the experiment lifecycle (system setup & configuration, execution, cleanup) manually, or write a bunch of glue code that does this automatically for you. In a distributed setting, each of these phases is susceptible to occasional errors, so your glue code will need a couple of iterations until it becomes robust enough so you can rely on it.

### Analysis Code

When you come to [Step 4](#step-4-result-analysis--closing-the-loop), you will have to extract the data observed at execution time. Since every system has it’s own logging format and specifics, you will have to write more glue code that deals with that, most likely handling the specific problem at hand.

### Packaging &amp; Sharing

After fiddling around for some days, you will solve the problem at hand and produce some nice charts to share. [*Repeatability* and *reproducibility*, however, are important properties of any good experiment](http://www.tpc.org/tpctc/tpctc2009/tpctc2009-03.pdf), and you might be asked to package and share the experiments code with a skeptical third party. You will have to make sure that everything works well on another developer machine and another execution environment (e.g. EC2).

## Solution

Peel can help you solve all of the above problems. The remainder of this manual explains how by means of the running example presented above. To get the example, run the following code snippet:

{% highlight bash %}
cd "$BUNDLE_SRC"
mvn archetype:generate -B                         \
    -Dpackage="org.peelframework.wordcount"       \
    -DgroupId="org.peelframework"                 \
    -DartifactId="peel-wordcount"                 \
    -DarchetypeGroupId=org.peelframework          \
    -DarchetypeArtifactId=peel-flinkspark-bundle  \
    -DarchetypeVersion={{ site.current_version }}
cd "peel-wordcount"
mvn clean deploy
cd "$BUNDLE_BIN/peel-wordcount"
{% endhighlight %}