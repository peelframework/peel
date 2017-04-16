---
layout: post
title:  Hash Aggregations in Flink
author: 
 - Alexander Alexandrov
 - Gábor Gévay
 - Andreas Kunft
date:   2016-04-07 14:28:46
nav: blog
---

Hash-based aggregations have been a long-standing item on the feature list for Flink (see [FLINK-2237](https://issues.apache.org/jira/browse/FLINK-2237)).
We recently submitted a PR implementing the first step towards providing this fuctionality with [FLINK-3477](https://issues.apache.org/jira/browse/FLINK-3477), which introduces a hash-based aggregation strategy for combiners.
In this post, we review the main differences between sort- and hash-based aggregations and present a Peel-based experiments bundle which analyzes the benefits of the hash-based strategy.

## Design Space

The **sort-based combiner** collects input elements into a sorted buffer. 
When the buffer runs out of memory, it combines the elements in a single pass over the data and emits the resulting partial aggregates.

The **hash-based combiner** also emits all data that it currently has upon running out of memory. 
In contrast to the sort algorithm, however, it performs combine steps eagerly at the arrival of each input element. 

The hash-based combiner, therefore, consumes memory proportional to the number of groups, while the sort-based consumes memory proportional to the number of elements.
This results in less frequent emissions, and more combine steps performed input element pairs.

To evaluate and analyze the relative merits of the hash and the sort-based aggregates, we propose the following experimental setup.

## Bundle Design

To analyze the performance impact of the conceptual differences between the two aggregation strategies outlined above, we developed an experiment bundle designed as follows.

### Data Sets

The datasets in use are a collections of key-value pairs of type `(Long, String)` that are generated according to the following schema.

* Value lengths are distributed uniformly in the range *[10, 20]*. The number of possible values is 1000000.
* The number of distinct keys *K*, as long as their distribution can be manipulated via data generation parameter *P*. Supported distributions are *Uniform*, *Binomial*, and *Zipf*.
* The size of the dataset is *E * N*, where *N* is the degree of parallelism and *E* defines the number of elements generated per worker.

We define three dataset families based on the above schema by keeping *E* fixed and scaling *K* by an order of magnitude twice.

#### Dataset A1

With *E = 40000000* and *K = 40000*.

#### Dataset A2

With *E = 40000000* and *K = 400000*.

#### Dataset A3

With *E = 40000000* and *K = 4000000*.

### Workloads

We have to basic workloads that execute simple MapReduce jobs.

#### Workload X

Compute length of the largest value per group.

```scala
env
  .readCsvFile[(Long, String)](inputPath)
  .map{kv => (kv._1, kv._2.length)}
  .groupBy(0)
  .reduce((x, y) => {
    val K = x._1
    val V = Math.max(x._2, y._2)
    (K, V)
  }, combineHint)
  .writeAsCsv(outputPath)
```

#### Workload Y

Get the largest value per group based on lexicographic ordering. This workload is designed to make the records change size during the reduce steps, so that the hash table can do an in-place update.

```scala
env
  .readCsvFile[(Long, String)](inputPath)
  .groupBy(0)
  .reduce((x, y) => {
    val K = x._1
    val V = if (x._2 > y._2) x._2 else y._2)
    (K, V)
  }, combineHint)
  .writeAsCsv(outputPath)
```

Both jobs are parameterizable with a `combineHint` parameter that sets a hash-based or a sort-based strategy for the combiner.

### Experiments

We define a family of experiments **[DS].[W]** by matching each of the three datasets *DS = { A1, A2, A3 }* with each of the two workloads *W = { X, Y }*.

#### Experiment [DS].[W]

Run **Workload [W]** on top of **Dataset [DS]** using the following distributions.

* *Uniform*,
* *Binomial*, with success probability *0.5*, and
* *Zipf*, with support *1*.

To analyze performance, we are interested in the following experiment aspects:

* execution Time,
* number of emissions, and 
* average amount of tuples sent per emission.

## Bundle Implementation

The experimental setup described above is realized as [a **flink-hashagg** Peel bundle](https://github.com/TU-Berlin-DIMA/flink-hashagg) using the *peel-flink-bundle* archetype. The bundle contains the following elements.

### Systems

The system under test is [a custom Flink build based on FLINK-3477](https://github.com/stratosphere/flink/releases/tag/v1.1-FLINK-3477). 
In addition to the hash-based combiner strategy, the build contains [some additional code modifications](https://github.com/stratosphere/flink/commit/d1e6ea7ea80038df5ecf52550ad73c2774749395) in order to log extra statistics, such as the number of emissions and the total number of emitted records per combiner.

The system is integrated into the bundle by means of the following additions.

* A [system bean definition](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/systems.scala#L25-L32) for **flink-1.1-FLINK-3477**.
* A [reference configuration](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-peelextensions/src/main/resources/reference.flink-1.1-FLINK-3477.conf) located in the **flink-hashagg-peelextensions** module.
* A [bundle-specific configuration](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/flink-1.1-FLINK-3477.conf) located in the **f****link-hashagg-bundle** module.

### Datasets

The [data generator for **Datasets A1-A3**](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-datagens/src/main/scala/de/tu_berlin/dima/experiments/flink/hashagg/datagen/flink/DatasetAGenerator.scala) is realized as a Flink job located in the **flink-hashagg-datagens** module. The job basically maps a sequence of pseudo-random numbers to a (pseudo-random) sequence of key-value pairs. To make this task embarrassingly parallel, we use a special [pseudo-random number generator](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-datagens/src/main/java/de/tu_berlin/dima/experiments/flink/hashagg/datagen/util/RanHash.java) that can efficiently skip to any offset given a fixed seed.

Our experimental setup relies on generated instances of **Dataset A1-A3** with *P ∈ { Uniform, Binomial[0.5], Zipf[1] }*. We therefore define three groups of beans for [**Dataset A1**](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/datasets.scala#L55-L65), [**Dataset A2**](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/datasets.scala#L69-L79), and [**Dataset A3**](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/datasets.scala#L83-L93), respectively. The bean definitions delegate to common factory methods that produce the corresponding [GeneratedDataSet](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/datasets.scala#L47-L51) and [FlinkJob](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/datasets.scala#L29-L41) instances.

In addition, we also define [two ExperimentOutput beans](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/fixtures/datasets.scala#L99-L109) for the outputs of the two workload jobs.

### Workload Jobs

[**Workload X**](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-flink-jobs/src/main/scala/de/tu_berlin/dima/experiments/flink/hashagg/flink/WorkloadX.scala) and [**Workload Y**](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-flink-jobs/src/main/scala/de/tu_berlin/dima/experiments/flink/hashagg/flink/WorkloadY.scala) are both implemented in Scala using Flink’s DataSet API.

### Experiments

Tying all of the above elements together, we finally define experiment suites [for each **Experiment [DS].[W]** variant](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-bundle/src/main/resources/config/experiments.scala#L82-L104).

In order to trigger multiple combiner emissions even with the relatively small **Dataset A**, we have to reduce the memory available to the Flink runtime. To that end, we override the relevant Flink config parameters in the suite’s experiment configs as follows.


```docker
system.flink.config.yaml {
  # 1 GiB of memory
  taskmanager.heap.mb = 1024
  # 0.5 * 1 = 0.5 GiB will be managed
  taskmanager.memory.fraction = 0.5
  # 16384 * 16384 = 0.25 GiB memory for network
  taskmanager.network.numberOfBuffers = 16384
  taskmanager.network.bufferSizeInBytes = 16384
}
```

Each task manager operates with 1GiB of total and 0.5GiB of managed memory, from which 0.25GiB is reserved for network buffers.

### Result Analysis Utilities

To streamline the result analysis process we define several extra beans in the **flink-hashagg-peelextensions** module. 
To extract the additional combiner metrics from the task manager logs, we add [a *CombinerMetricsExtractor* component](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-peelextensions/src/main/scala/de/tu_berlin/dima/experiments/flink/hashagg/results/etl/extractor/CombinerMetricsExtractor.scala), and to print results and metrics from per suite, we add [a *QueryResults* CLI command](https://github.com/TU-Berlin-DIMA/flink-hashagg/blob/v1.0.0/flink-hashagg-peelextensions/src/main/scala/de/tu_berlin/dima/experiments/flink/hashagg/cli/command/QueryResults.scala). 
Annotating these types with `@Component` or `@Serivice` exposes them to the Peel auto-discovery system, so the corresponding beans are automatically configured.

## Execution Environment

The experiments were executed on a dedicated cluster (called `cloud-7`) consisting of four Ubuntu 14.04 nodes, running HDFS 2.7.1 and our modified version of Flink. 
Each node has a 2.40GHz Xeon(R) E5530 CPU with 8 hyper-threaded cores, resulting in a total available degree of parallelism of 64. 
The master node has 64 GiB and the remaining three nodes 48 GiB of DIMM RAM. 
All nodes are connected through a 1000 Mbps ethernet connection. 
Each experiment was repeated 5 times in oder to account for outliers caused by external interference.

All [experiment results are available](https://github.com/TU-Berlin-DIMA/flink-hashagg/releases/tag/v1.0.0) on the project webpage.

## Results Analysis

Let us now discuss the results of the experiments. The graphs below show the median of the execution times of the 5 repeated runs of the experiments.

<ul class="tabs" data-tab>
    <li class="tab-title active"><a href="#runtimes-A1">A1</a></li>
    <li class="tab-title"><a href="#runtimes-A2">A2</a></li>
    <li class="tab-title"><a href="#runtimes-A3">A3</a></li>
</ul>
<div class="tabs-content">
    <figure class="content active" id="runtimes-A1">
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/runtimes.ex-A1.X.v1.0.0.cloud-7.svg" alt="runtimes.ex-A1.X.v1.0.0.cloud-7"/>
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/runtimes.ex-A1.Y.v1.0.0.cloud-7.svg" alt="runtimes.ex-A1.Y.v1.0.0.cloud-7"/>
    </figure>
    <figure class="content" id="runtimes-A2">
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/runtimes.ex-A2.X.v1.0.0.cloud-7.svg" alt="runtimes.ex-A2.X.v1.0.0.cloud-7"/>
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/runtimes.ex-A2.Y.v1.0.0.cloud-7.svg" alt="runtimes.ex-A2.Y.v1.0.0.cloud-7"/>
    </figure>
    <figure class="content" id="runtimes-A3">
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/runtimes.ex-A3.X.v1.0.0.cloud-7.svg" alt="runtimes.ex-A3.X.v1.0.0.cloud-7"/>
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/runtimes.ex-A3.Y.v1.0.0.cloud-7.svg" alt="runtimes.ex-A3.Y.v1.0.0.cloud-7"/>
    </figure>
</div>

Recall that when the combiner runs out of memory, it emits all partial aggregates that it currently holds. 
The other graphs show the average number of such emissions per run, and the average number of records emitted per task.

<ul class="tabs" data-tab>
    <li class="tab-title active"><a href="#tuples-and-emissions-A1">A1</a></li>
    <li class="tab-title"><a href="#tuples-and-emissions-A2">A2</a></li>
    <li class="tab-title"><a href="#tuples-and-emissions-A3">A3</a></li>
</ul>
<div class="tabs-content">
    <figure class="content active" id="tuples-and-emissions-A1">
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/tuples.ex-A1.X.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/tuples.ex-A1.Y.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/emissions.ex-A1.X.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/emissions.ex-A1.Y.v1.0.0.cloud-7.svg" />
    </figure>
    <figure class="content" id="tuples-and-emissions-A2">
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/tuples.ex-A2.X.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/tuples.ex-A2.Y.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/emissions.ex-A2.X.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/emissions.ex-A2.Y.v1.0.0.cloud-7.svg" />
    </figure>
    <figure class="content" id="tuples-and-emissions-A3">
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/tuples.ex-A3.X.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/tuples.ex-A3.Y.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/emissions.ex-A3.X.v1.0.0.cloud-7.svg" />
        <img class="left large-6 medium-6 small-12 small-centered columns" src="{{ site.baseurl}}/assets/blog/flink-hashagg/emissions.ex-A3.Y.v1.0.0.cloud-7.svg" />
    </figure>
</div>

As expected, the number of emissions is much lower with the hash-based strategy. 
Concretely, it is always 1, because the memory usage of the hash-based strategy is proportional to the number of distinct keys, which even with the scarce memory budget was low enough here to not make the buffer of the combiner fill up.

This causes the number of emitted records to also be lower, since more such pairs of elements "meet" in the combiner that can be combined. 
In certain situations this might improve performance by reducing the network load (although the network was not saturated during these experiments).

In overall, the runtimes turned out to be better for the hash-based strategy. 
With the Uniform and the Binomial distributions, hashing was faster by about 20-30%, and with the Zipf distribution, sorting won by only a small margin.
In the latter case, it seems that the skewness of the Zipf distribution helps the sort-based strategy more than it helps the hash-based strategy.
A possible reason for this can be that significantly less swaps happen during the sorting when lots of elements have the same key.

If we compare the left and right columns, we can see that the two workloads have almost the same behaviour, which shows that it did not hurt the hash table’s performance that it could not always do an in-place update after the reduce steps.

## Repeatability

To repeat the experiments on a different environment, follow the instructions below.

The the following global variables as assumed.

```bash
# the bundles parent folder
export BUNDLE_BIN=~/bundles/bin 
# the hostname of the target exec. environment
export ENV=target-exec-env-hostname
# the experiments version tag
export VER=1.0.0
# repository URLs
export REP=https://github.com/TU-Berlin-DIMA/flink-hashagg
export URL=$REP/releases/download/v$VER
```

### Downloading the Bundle

The binary assembly of the **flink-hashagg** Peel bundle is available at the project webpage and can be downloaded as follows.

```bash
wget $URL/flink-hashagg.tar.gz
tar -xzvf flink-hashagg.tar.gz
cd flink-hashagg
```

### Downloading the `cloud-7` Results

If you wish to also download the results presented in this paper, execute the following commands.

```bash
# on the target execution environment ($ENV)
for DS in A1 A2 A3; do for W in X Y; do
    # download results for experiment
    wget -P results $URL/ex-$DS.$W.v$VER.cloud-7.tar.gz
    # extract results for experiment
    ./peel.sh res:extract ex-$DS.$W.v$VER.cloud-7
done; done
```

If you rather wish to analyze these results instead of repeating the experiments, proceed directly to ["Importing the Experiment Results"](#importing-the-experiment-results) using `ENV=cloud-7`.

### Repeating the Experiments

To run the experiments, you will then have to do the following changes to the bundle configuration.

1. Setup a host-based environment configuration for your target execution environment, [as discussed in the "Sharing Configurations" section of the Peel manual](http://peel-framework.org/manual/environment-configurations.html).
2. Configure the target environment connection parameters for bundle deployment, [as discussed in the "Bundle Deployment" section of the Peel manual](http://peel-framework.org/manual/execution-workflow.html#bundle-deployment).

Once you are done with that, deploy the bundle to your execution environment.

```bash
# on the developer host
./peel.sh rsync:push $ENV
```

Upon that, login on the target environment and execute the following commands.

```bash
# on the target execution environment ($ENV)
for DS in A1 A2 A3; do for W in X Y; do
  ./peel.sh suite:run ex-$DS.$W
done; done
```

Each experiment in the two suites will be repeated 5 times in order to protect against outliers caused by system warm up and external interference.

Upon executing the experiments, archive the result folders and fetch them locally as follows. On the target execution environment execute the following commands.

```bash
for DS in A1 A2 A3; do for W in X Y; do
  # append $VER and $ENV to results folder
  mv results/ex-$DS.$W results/ex-$DS.$W.v$VER.$ENV
  # archive results folder
  ./peel.sh res:archive ex-$DS.$W.v$VER.$ENV
done; done
```

And after that the developer host execute this.

```bash
# pull the results from the target execution environment ($ENV)
./peel.sh rsync:pull cloud-7-peel
# extract the result folders for local analysis
for DS in A1 A2 A3; do for W in X Y; do
    ./peel.sh res:extract ex-$DS.$W.v$VER.$ENV
done; done
```

### Importing the Experiment Results

Before you can import the results you will have to prepare a MonetDB instance, [following the instructions from the manual section](http://peel-framework.org/manual/results-analysis.html#monetdb). 
The instance schema can be then initialized as follows.

```bash
./peel.sh db:initialize -f --connection monetdb
```

After that, use the following command to import the results.

```bash
for DS in A1 A2 A3; do for W in X Y; do
  ./peel.sh db:import  ex-$DS.$W.v$VER.$ENV --connection monetdb
done; done
```

### Generating the Result Plots

To generate the SVG plots, use this.

```bash
for DS in A1 A2 A3; do for W in X Y; do
  ./peel.sh db:results ex-$DS.$W.v$VER.$ENV --connection monetdb
done; done
```

PNG charts with side-by-side comparison between workloads A and B can be then derived from the SVG files using ImageMagick.

```bash
# convert SVG to PNG
for f in $(find -name '*.svg'); do convert $f ${f%.svg}.png; done
# create comparison figures folder
mkdir ./results/comparison/
# combine plots
for DS in A1 A2 A3; do for T in runtimes tuples emissions; do
  montage -geometry +0+0 \
    ./results/ex-$DS.X.v$VER.$ENV/plots/$T.png \
    ./results/ex-$DS.Y.v$VER.$ENV/plots/$T.png \
    ./results/comparison/ex-$DS.v$VER.$ENV.$T.png
done; done
```
