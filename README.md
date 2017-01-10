Peel Experiments Execution Framework
====================================

[![Build Status](https://travis-ci.org/peelframework/peel.svg?branch=dev)](https://travis-ci.org/peelframework/peel)

Peel is a framework that helps you to define, execute, analyze, and share experiments for distributed systems and algorithms.

For more information and technical documentation about the project, please visit [peel-framework.org](http://peel-framework.org).

Check the [Motivation section on our website](http://peel-framework.org/manual/motivation.html) to understand the problems Peel will solve for you.

Check the [Getting Started guide](http://peel-framework.org/getting-started.html) and the [Bundle Basics](http://peel-framework.org/manual/motivation.html) section.

<p align="center">
  <img src="http://peel-framework.org/img/peeled_mango_small.jpg" alt="Peel Mango" />
</p>

## Main Features

Peel offers the following features for your experiments.

- **Unified Design**. Specify and maintain collections of experiments using a simple, DI-based configuration.
- **Automated Execution**. Automate the experiment execution lifecycle.
- **Automated Analysis**. Extracts, transforms, and loads results into an RDBMS.
- **Result Sharing**. Share your bundles and migrating to other evaluation environments without additional effort.

## Supported Systems

| System           | Version        | System bean ID    |
| ---------------- | -------------- | ----------------- |
| HDFS             | 1.2.1          | `hdfs-1.2.1`      |
| HDFS             | 2.4.1          | `hdfs-2.4.1`      |
| HDFS             | 2.7.1          | `hdfs-2.7.1`      |
| Flink            | 0.8.0          | `flink-0.8.0`     |
| Flink            | 0.8.1          | `flink-0.8.1`     |
| Flink            | 0.9.0          | `flink-0.9.0`     |
| Flink            | 0.10.0         | `flink-0.10.0`    |
| Flink            | 0.10.1         | `flink-0.10.1`    |
| Flink            | 0.10.2         | `flink-0.10.2`    |
| Flink            | 1.0.0          | `flink-1.0.0`     |
| Flink            | 1.0.1          | `flink-1.0.1`     |
| Flink            | 1.0.2          | `flink-1.0.2`     |
| Flink            | 1.0.3          | `flink-1.0.3`     |
| Flink            | 1.1.0          | `flink-1.1.0`     |
| Flink            | 1.1.1          | `flink-1.1.1`     |
| Flink            | 1.1.2          | `flink-1.1.2`     |
| Flink            | 1.1.3          | `flink-1.1.3`     |
| Flink            | 1.1.4          | `flink-1.1.4`     |
| MapReduce        | 1.2.1          | `mapred-1.2.1`    |
| MapReduce        | 2.4.1          | `mapred-2.4.1`    |
| Spark            | 1.3.1          | `spark-1.3.1`     |
| Spark            | 1.4.0          | `spark-1.4.0`     |
| Spark            | 1.4.1          | `spark-1.4.1`     |
| Spark            | 1.5.1          | `spark-1.5.1`     |
| Spark            | 1.5.2          | `spark-1.5.2`     |
| Spark            | 1.6.0          | `spark-1.6.0`     |
| Spark            | 1.6.2          | `spark-1.6.2`     |
| Spark            | 2.0.0          | `spark-2.0.0`     |
| Spark            | 2.0.1          | `spark-2.0.1`     |
| Spark            | 2.0.2          | `spark-2.0.2`     |
| Spark            | 2.1.0          | `spark-2.1.0`     |
| Zookeeper        | 3.4.5          | `zookeeper-3.4.5` |
| Dstat            | 0.7.2          | `dstat-0.7.2`     |

## Supported Commands

| Command              | Description                                        |
| -------------------- | -------------------------------------------------- |
| `db:import`          |  import suite results into an initialized database |
| `db:initialize`      |  initialize results database                       |
| `exp:run`            |  execute a specific experiment                     |
| `exp:setup`          |  set up systems for a specific experiment          |
| `exp:teardown`       |  tear down systems for a specific experiment       |
| `exp:config`         |  list the configuration of a specific experiment   |
| `hosts:generate`     |  generate a hosts.conf file                        |
| `res:archive`        |  archive suite results to a tar.gz                 |
| `res:extract`        |  extract suite results from a tar.gz               |
| `rsync:pull`         |  pull bundle from a remote location                |
| `rsync:push`         |  push bundle to a remote location                  |
| `suite:run`          |  execute all experiments in a suite                |
| `sys:setup`          |  set up a system                                   |
| `sys:teardown`       |  tear down a system                                |
| `val:hosts`          |  validates correct hosts setup                     |

