---
layout: manual
title: Environment Configurations
date: 2015-07-06 10:00:00
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

The naïve approach therefore puts a substantial burden on the person conducting the experiments. 

## Approach

Peel’s approach towards the difficulties outlined above is to associate *one global environment configuration* to each experiment. In doing this, Peel promotes

1. configuration *reuse* through *layering*, as well as
1. configuration *uniformity* through a *hierarchical syntax ([HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md))*.

At runtime, experiments are represented by [experiment beans]({{ site.baseurl }}/manual/experiments-definitions.html#experiment). 
Each experiment bean holds a *HOCON config* that is first constructed and evaluated based on the layering scheme and conventions discussed below, and then mapped to the various concrete *config* and *parameter* files and formats of the systems and applications in the experiment environment.

In our running example, this means that each of the six experiments (3x `SparkWC` + 3x `FlinkWC`) will have an associated `config` property -- a hierarchical map of key-value pairs which constitute the configuration of all systems and jobs required for that particular experiment.

<div class="row">
    <figure class="large-10 large-centered medium-11 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc_mapping.svg" title="Mapping the environment configuration for the WC-Spark experiment" alt="Mapping the environment configuration for the WC-Spark experiment" /><br />
        <figcaption>Mapping the environment configurations for the six Wordcount experiments</figcaption>
    </figure>
</div>

## Configuration Layers

The Peel configuration system is built upon the concept of layered construction and resolution. Peel distinguishes between three layers of configuration:

1. **Default**. Default configuration values for Peel itself and the supported systems. Packaged as resources in Peel-related jars located in the bundle's `app.path.lib` folder.
2. **Bundle.** Bundle-specific configuration values. Located in `config/hosts`. Default is the `config/hosts` subfolder of the current bundle.
3. **Host**. Host-specific configuration values. Located in the `$HOSTNAME` subfolder of the `app.path.config` folder.

For each experiment bean defined in an experiment suite, Peel will construct an associated configuration according to the following table entries (higher in the list means lower priority).

| Path                                        | Description                                |
| ------------------------------------------- | -------------------------------------------|
| `reference.peel.conf`                       | Default Peel config.                       |
| `reference.${systemID}.conf`                | Default system config.                     |
| `config/${systemID}.conf`                   | Bundle-specific system config (opt).       |
| `config/hosts/${hostname}/${systemID}.conf` | Host-specific system config (opt).         |
| `config/application.conf`                   | Bundle-specific Peel config (opt).         |
| `config/hosts/${hostname}/application.conf` | Host-specific Peel config (opt).           |
| Experiment bean *config* value              | Experiment specific config (opt).          |
| *System*                                    | JVM system properties (constant).          | 

First comes [the default Peel configuration](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/resources/reference.peel.conf), located in the `peel-core.jar` package.

Second, for each system upon which the experiment depends (with corresponding [system bean]({{ site.baseurl }}/manual/experiments-definitions.html#system) identified by `systemID`), Peel tries to loads the the default configuration for that system as well as bundle- or host-specific configurations.

Third, bundle- and host-specific `application.conf`, which is a counterpart and respectively overrides bundle-wide values defined in `reference.peel.conf`.

Upon that come the values defined the `config` property of the current experiment bean. These are typically used to vary one particular parameter in a sequence of experiment in a suite (e.g. varying the number of workers and the DOP).

Finally, Peel appends a set of configuration parameters derived from the current JVM System object (e.g., the number of CPUs or the total amount of available memory).

*__Tip__: You can see the sequence of files loaded as part of the construction of a particular configuration environment in the Peel console log at runtime.*

## Sharing Configurations

One of the main advantages of Peel is the ability to share hand-crafted configurations for a set of systems on a particular host environment.
The suggested way to do so is through a dedicated Git repository. 

If you are using a versioned bundle, must clone the repository under your `*-bundle` project.

For example, we offer an example ACME cluster has a shared configuration available [at GitHub](https://github.com/stratosphere/peelconfig.acme), you can use the following command to add it to your `peel-wordcount` bundle:

{% highlight bash %}
git clone \
    git@github.com:stratosphere/peelconfig.acme.git \
    peel-wordcount-bundle/src/main/resources/config/hosts/acme-master
{% endhighlight %}

We also encourage beginners to use the [devhost config](https://github.com/stratosphere/peelconfig.devhost) which contains best practice configurations for your developer machine.

{% highlight bash %}
git clone \
    git@github.com:stratosphere/peelconfig.devhost.git \
    peel-wordcount-bundle/src/main/resources/config/hosts/$HOSTNAME
{% endhighlight %}

If you intend to modify those settings, we suggest to fork the repository and clone the fork instead.

Please check the [Environment Configurations Repository]({{ site.baseurl }}/repository/environment-configurations.html) for more information on that matter and a list of available configuration repositories.

## Example

Let us take a look at the `config` folder of the `peel-wordcount` bundle in order to illustrate some of the concepts presented above.

{% highlight bash %}
# cd "$BUNDLE_BIN" && \
# tree -L 3 --dirsfirst peel-wordcount/config
peel-wordcount/config
├── hosts
│   ├── acme-master
│   │   ├── application.conf
│   │   ├── flink-0.8.0.conf
│   │   ├── flink-0.8.1.conf
│   │   ├── flink-0.9.0.conf
│   │   ├── flink.conf
│   │   ├── hadoop-2.conf
│   │   ├── hdfs-2.4.1.conf
│   │   ├── hdfs-2.7.1.conf
│   │   ├── hosts.conf
│   │   ├── spark-1.3.1.conf
│   │   └── spark.conf
│   └── $HOSTNAME
│       ├── application.conf
│       ├── hadoop-2.conf
│       ├── hdfs-2.4.1.conf
│       └── hdfs-2.7.1.conf
├── experiments.wordcount.xml
├── experiments.xml
└── systems.xml
{% endhighlight %}

We can see that our bundle does not include bundle-wide environment configuration, because the `config` folder does not contain any `*.conf` files as direct children. 

On the other side, we have two host-specific configurations in the `hosts` subfolder.

Under the `$HOSTNAME` of your machine we can see the following `*.conf` files:

* A host-specific system configuration for the following system beans:
  * [hdfs-2.4.1](https://github.com/stratosphere/peelconfig.devhost/blob/master/hdfs-2.4.1) which delegates to [hadoop-2.conf](https://github.com/stratosphere/peelconfig.devhost/blob/master/hadoop-2.conf), and
  * [hdfs-2.7.1](https://github.com/stratosphere/peelconfig.devhost/blob/master/hdfs-2.7.1) which delegates to [hadoop-2.conf](https://github.com/stratosphere/peelconfig.devhost/blob/master/hadoop-2.conf).
* A [host-specific Peel configuration](https://github.com/stratosphere/peelconfig.devhost/blob/master/application.conf) which sets a shared `downloads` and `systems` paths and configures an rsync connection to the ACME cluster.

Under `acme-master` (which is the `$HOSTNAME` of the master node in the ACME cluster) we can see the following `*.conf` files:

* A host-specific system configuration for the following system beans:
  * [flink-0.8.0](https://github.com/stratosphere/peelconfig.acme/blob/master/flink-0.8.0.conf) which delegates to [flink.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/flink.conf),
  * [flink-0.8.1](https://github.com/stratosphere/peelconfig.acme/blob/master/flink-0.8.1.conf) which delegates to [flink.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/flink.conf),
  * [flink-0.9.0](https://github.com/stratosphere/peelconfig.acme/blob/master/flink-0.9.0.conf) which delegates to [flink.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/flink.conf),
  * [spark-1.3.1](https://github.com/stratosphere/peelconfig.acme/blob/master/spark-1.3.1) which delegates to [spark.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/spark.conf),
  * [hdfs-2.4.1](https://github.com/stratosphere/peelconfig.acme/blob/master/hdfs-2.4.1) which delegates to [hadoop-2.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/hadoop-2.conf), and
  * [hdfs-2.7.1](https://github.com/stratosphere/peelconfig.acme/blob/master/hdfs-2.7.1) which delegates to [hadoop-2.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/hadoop-2.conf).
* A [host-specific Peel configuration](https://github.com/stratosphere/peelconfig.acme/blob/master/application.conf) which includes [an auto-generated hosts.conf for the ACME cluster](https://github.com/stratosphere/peelconfig.acme/blob/master/hosts.conf).

When we run Peel experiment suites from the `acme-master` host, we will therefore use the host-specific environment configuration values defined under `config/acme-master`. Otherwise, Peel will load and use only the default `reference.*.conf` files from the `peel-core` and `peel-extensions` jars.
