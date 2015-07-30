---
layout: manual
title: Bundle Basics
date: 2015-07-05 10:00:00
nav: [ manual, bundle-basics]
---

# Bundle Basics

A Peel package bundles together the configuration data, datasets, and programs required for the execution of a particular set of experiments, and is therefore shortly referred to as a Peel bundle.

## Folder Structure

The top-level elements of a Peel bundle are shortly summarized below.

| Path      | Config Parameter     | Description                                                   |
| --------- | -------------------- | ------------------------------------------------------------- |
| apps      | `app.path.apps`      | Workload applications.                                        |
| config    | `app.path.config`    | Environment configurations and experiments definitions.       |
| datagens  | `app.path.datagens`  | Data generators.                                              |
| datasets  | `app.path.datasets`  | Static datasets.                                              |
| downloads | `app.path.downloads` | Archived system binaries.                                     |
| lib       | `N/A`                | Peel libraries and dependencies.                              |
| log       | `app.path.log`       | Peel execution logs.                                          |
| results   | `app.path.results`   | State and log data associated with attempted experiment runs. |
| systems   | `app.path.systems`   | Contains all running systems.                                 |
| peel.sh   | `N/A`                | The Peel command line interface.                              |

You can customize the [default paths](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/resources/reference.peel.conf) of these elements in your bundle- or host-specific configuration. For more information how to do this, please refer to the [Environment Configurations]({{ site.baseurl }}/manual/environment-configurations.html) section.

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

The downloads folder contains system binary archives for the systems in your experiment environment managed by Peel. The system archives are per default extracted in the systems folder.

### Results

The *results* folder contains data collected from attempted successful Peel experiment runs in a hierarchy following a *${suite}/${expName}.run${NN}* naming convention.

## Example

[TODO]