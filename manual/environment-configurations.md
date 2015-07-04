---
layout: manual
title: Environment Configurations
nav: [ manual, environment-configurations ]
---

# {{ page.title }}

This section discusses Peel’s approach of a unified, global experiment environment configuration.

## Challenges

In the [Motivation]({{ site.baseurl }}/manual/motivation.html) section, we defined an experiment environment as a combination of

1. a host environment,
2. a set of dependent systems, amongst which a principle system under test, and
3. an experiment application.

Environments are instantiated with a concrete set of configuration values (for the systems) and parameter values (for the experiment application). Here is again a figure of the environments created for a series of experiments in our [running example]({{ site.baseurl }}/manual/motivation.html#running-example).

<div class="row">
    <figure class="large-9 large-centered medium-10 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc_series.svg" title="Environment layouts for 'Weak Scale-Out - Spark vs. Flink'" alt="Environment layouts for 'Weak Scale-Out - Spark vs. Flink'" /><br />
        <figcaption>Environment layouts for "Weak Scale-Out - Spark vs. Flink"</figcaption>
    </figure>
</div>

A number of problems arise with a naïve approach for manual configuration (per system & experiment) of the environments in the above example:

1. **Syntax Heterogeneity.** Each system ([HDFS](http://hadoop.apache.org/docs/r2.7.0/hadoop-project-dist/hadoop-common/ClusterSetup.html#Slaves_File), [Spark](https://spark.apache.org/docs/latest/spark-standalone.html#cluster-launch-scripts) & [Flink](http://ci.apache.org/projects/flink/flink-docs-release-0.9/setup/config.html)) has to be configured separately using its own special syntax. This requires basic understanding and knowledge in the configuration parameters for all systems in the stack.
1. **Variable Interdependence.** The sets of configuration variables associated with each system are not mutually exclusive, and care has to be taken that the corresponding values are consistent for the overlapping fragment (e.g., the *slaves* list in all systems should be the same).
1. **Value Tuning.** For a series of related experiments, all but a very few set of values remain fixed. These values are suitably chosen based on the underlying host environment characteristics in order to maximise the performance of the corresponding systems (e.g., memory allocation, degree of parallelism, temp paths for spilling).

## Approach

The naïve approach therefore puts a substantial burden on the person conducting the experiments. Peel’s approach towards the difficulties outlined above is to associate *one global environment configuration* to each experiment. In doing this, Peel promotes

1. configuration *reuse* through *layering*, as well as
1. configuration *uniformity* through a *hierarchical syntax ([HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md))*.

At runtime, the *experiment config* is first lazily constructed and evaluated based on the layering scheme and conventions discussed below, and then (partially) mapped to the various concrete *config* and *parameter* files and formats of the systems and applications in the experiment environment.

<div class="row">
    <figure class="large-10 large-centered medium-11 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc_mapping.svg" title="Mapping the environment configuration for the WC-Spark experiment" alt="Mapping the environment configuration for the WC-Spark experiment" /><br />
        <figcaption>Mapping the environment configuration for the WC-Spark experiment</figcaption>
    </figure>
</div>

## Configuration Layers

The Peel configuration system is built upon the concept of layered construction and resolution. Peel distinguishes between three layers of configuration:

1. **Default**. Default configuration values for Peel itself and the supported systems. Packaged in Peel-related jars.
2. **Bundle.** Bundle-specific configuration values. Located in a folder resolved via the Java system property `app.path.config` at runtime. Default is the *config* folder of the current bundle.
3. **Host**. Host-specific configuration values. Located in the `hostname` subfolder of the `app.path.config` folder.

For each experiment in a suite, Peel will construct an associated experiment configuration according to the following table entries (higher in the list means lower priority).

| Path                                              | Description                                      |
| ------------------------------------------------- | ------------------------------------------------ |
| `resource:reference.conf`                         | Default Peel configuration.                      |
| `reference.${systemID}.conf`                      | Default system configuration.                    |
| `${app.path.config}/${systemID}.conf`             | Bundle-specific system configuration (optional). |
| `${app.path.config}/${hostname}/${systemID}.conf` | Host-specific system configuration (optional).   |
| `${app.path.config}/application.conf`             | Bundle-specific Peel configuration (optional).   |
| `${app.path.config}/${hostname}/application.conf` | Host-specific Peel configuration (optional).     |
| Experiment bean *config* value                    | Experiment specific configuration (optional).    |
| *System*                                          | JVM system properties (constant).                |

First comes the default Peel configuration.
Second, for each system upon which the experiment depends (with bean identified by `systemID`), the corresponding default system configuration as well as bundle- or host-specific versions.
Third, bundle- and host-specific `application.conf` (counterpart of `reference.conf`).
Fourth, experiment-specific configuration overrides and parameters as defined in the *config* property of the corresponding experiment bean.
Finally, a set of configuration parameters derived from the current JVM System object.

*__Tip__: You can see the sequence of files loaded as part of the construction of a particular configuration environment in the Peel console log at runtime.*

## Sharing Configurations

One of the main advantages of Peel is the ability to share hand-crafted configurations for a particular set of systems on a particular host environment.
The suggested way to do so is through a dedicated repository for each host-specific configuration.
Please check the [Environment Configurations Repository]({{ site.basepath }}/repository/environment-configurations.html) for more information on that matter.

## Example

Take a look at the config folder of the WordCount bundle. [TODO]