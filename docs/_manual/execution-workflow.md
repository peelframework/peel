---
layout: manual
title: Execution Workflow
date: 2015-07-08 10:00:00
nav: [ manual, execution-workflow ]
---

# {{ page.title }}

In the previous sections we explained the internals and the code required to configure the environment and define the experiments in a Peel bundle.
At this point, you should be able to figure out the `*.xml` and `*.conf` files in the `peel-wordcount-bundle` module.

In this section, we will explain how to make use of the commands provided by the Peel CLI in order to deploy and run the experiments in your bundle.

## Building the Bundle

To assemble the bundle from the sources with Maven, you have one of four options:

{% highlight bash %}
mvn clean package       # package
mvn clean package -Pdev # package with soft links
mvn clean deploy        # package and copy to $BUNDLE_BIN
mvn clean deploy -Pdev  # package and copy with soft links
{% endhighlight %}

The bundle binaries are assembled under `peel-wordcount-bundle/target`.

Running the `deploy` phase automatically copies the assembled bundle binaries folder to `$BUNDLE_BIN`.

Activating the `dev` profile with `-Pdev` enforces certain folders in the assembled bundle binaries (*config*, *datasets*, and  *utils*) to be created as soft links to their corresponding source locations.
We recommend this feature for bundles which are still under development, as it allows you to adapt the `*.xml` and `.conf` files on the fly from your IDE without rebuilding.


## Bundle Deployment

For large-scale applications, the environment where the experiments need to be executed typically differs from the environment of the machine where the bundle binaries are assembled.
In order to start the execution process, the user therefore needs to first deploy the bundle binaries from the local machine to the desired host environment.
The Peel CLI offers a special command to that purpose. In order to push the `peel-wordcount` bundle to the ACME cluster, you just have to run:

{% highlight bash %}
# from $BUNDLE_BIN/peel-experiments on the developer host
./peel.sh rsync:push acme
{% endhighlight %}

The command uses [rsync](https://en.wikipedia.org/wiki/Rsync) to copy the contents of the enclosing Peel bundle to the target environment. 
The connection options for the `rsync` calls are thereby taken from the environment configuration of the local environment.
In the example above we are referring to the `acme` remote, which is configured in the `application.conf` of the linked [developer environment](https://github.com/stratosphere/peelconfig.devhost/blob/master/application.conf#L22).

{% highlight docker %}
# rsync remotes, located in $HOSTNAME/application.conf
app.rsync {
    # 'ACME' remote 
    acme {
        url = "acme-master.acme.org"        # remote host url
        rsh = "ssh -l acme-user"            # remote shell to use
        dst = "/home/acme-user/experiments" # remote destination base folder
        own = "peel:peel"                   # remote files owner (optional)
    }
}
{% endhighlight %}

Upon executing the `rsync:push` command, you can login to the ACME server and continue working from there with the mirrored copy of your bundle.

{% highlight bash %}
# on the developer host
ssh -l acme-user acme-master.acme.org
# on acme-master.acme.org
cd "/home/acme-user/experiments/peel-wordcount"
{% endhighlight %}

Once you start [running experiments](#experiments-execution) on the remote host you will accumulate results data under the `app.path.results` folder of your bundle. 
Once you [archive those results](#archiving-the-results), you can fetch them back to your developer host with the following command.

{% highlight bash %}
# from $BUNDLE_BIN/peel-experiments on the developer host
./peel.sh rsync:pull acme
{% endhighlight %}

## Experiments Execution

The central piece of execution logic offered by Peel is the actual process of running the defined experiments.

### Execution Lifecycle

As we saw in the [Experiments Definitions]({{ site.baseurl }}/manual/experiments-definitions.html#experiment-suite) section, Peel organizes experiments in sequences called *experiment suites*. 
The execution lifecycle is therefore always tied to a specific suite and in general undergoes the following phases:

1. **Setup Suite**.
    * Systems with *Suite* lifespan required for execution are set up and started by Peel. 
    * Systems with *Provided* lifespan are assumed to be already up and running, but are re-configured if necessary.
1. **Execute Experiments**. For each experiment in the suite Peel performs the following tasks
    1. **Setup Experiment**.
        1. Ensure that the required inputs are materialized (either generated or copied) in the respective file system.
        1. Check the configuration of associated descendant systems with *Provided* or *Suite* lifespan against the values defined in the current experiment config. If the values do not match, it reconfigures and restarts the system.
        1. Set up systems with *Experiment* lifespan.
    1. **Execute Experiment Runs**. For each experiment run which has not been completed by a previous invocation of the same suite.
        1. **Setup Experiment Run**. 
            * Check and set up systems with *Run* lifespan.
        1. **Execute Experiment Run**. 
            1. Execute the experiment.
            1. Collect log data from the associated systems.
            1. Clear the produced outputs.
        1. **Tear Down Experiment Run**. 
            * Tear down systems with *Run* lifespan.
    1. **Tear Down Experiment**.
        * Tear down systems with *Experiment* lifespan.
1. **Tear Down Suite**.
    * Tear down systems with *Suite* lifespan.
    * Leaves systems with *Provided* lifespan up and running with the current configuration.

The Peel CLI offers a number of commands that are based on the above lifecycle.

### Running a Full Suite

First, let us take a look at the most common scenario - running all experiments in a suite. The following command

{% highlight bash %}
./peel.sh suite:run wordcount.scale-out
{% endhighlight %}

will execute the weak `wordcount.scale-out` experiment defined in the previous section.
The results are thereby written in the `results` subfolder of your bundle (the default path configured for `app.path.results`).
In this case the file structure looks as follows.

{% highlight bash %}
# tree -L 3 --dirsfirst results/wordcount.scale-out/
results/wordcount.scale-out/
├── datagen.words
│   ├── run.err
│   └── run.out
├── wordcount.flink.top005.run01
│   ├── logs
│   │   ├── flink
│   │   └── hdfs-2
│   ├── run.err
│   ├── run.out
│   ├── run.pln
│   └── state.json
├── wordcount.flink.top005.run02
│   └── ...
├── wordcount.flink.top005.run03
│   └── ...
├── wordcount.flink.top010.run01
│   └── ...
├── wordcount.flink.top010.run02
│   └── ...
├── wordcount.flink.top010.run03
│   └── ...
├── wordcount.flink.top020.run01
│   └── ...
├── wordcount.flink.top020.run02
│   └── ...
├── wordcount.flink.top020.run03
│   └── ...
├── wordcount.spark.top005.run01
│   ├── logs
│   │   ├── hdfs-2
│   │   └── spark
│   ├── run.err
│   ├── run.out
│   └── state.json
├── wordcount.spark.top005.run02
│   └── ...
├── wordcount.spark.top005.run03
│   └── ...
├── wordcount.spark.top010.run01
│   └── ...
├── wordcount.spark.top010.run02
│   └── ...
├── wordcount.spark.top010.run03
│   └── ...
├── wordcount.spark.top020.run01
│   └── ...
├── wordcount.spark.top020.run02
│   └── ...
└── wordcount.spark.top020.run03
    └── ...
{% endhighlight %}

Under `results/wordcount.scale-out` we see a folder for the data generation job `datagen.words`, as well as a folder for each experiment run.
The latter contains at least the following items:

* A `state.json` file with the serialized state of the experiment bean.
* A `run.out` file with the `stdout` putput of the experiment run.
* A `run.err` file with the `stderr` output of the experiment run.
* A `logs` folder with subfolders for each dependent system, containing the collected log data while running the experiment.

If an experiment run succeeds, the `runExitCode` value in it's `state.json` file is set to zero. A non-zero value marks an experiment as failed.
If you execute the `suite:run` command again, Peel will skip successfully completed runs per default.
In order to enforce the re-execution of successful experiments, use the `--force` flag.

{% highlight bash %}
./peel.sh suite:run wordcount.scale-out --force
{% endhighlight %}

### Running a Single Experiment

Sometimes you might want to execute a single experiment run in a suite. The Peel CLI command for that is `exp:run`.
This command essentially follows the same lifecycle as `suite:run`, but considers only a single run of the specified experiment to be part of the suite and neglects its `runExitCode` value.
The default run to be executed is 1, but you can change this with the optional `--run` argument.

For example, to execute only the second run of the `wordcount.flink.top010` experiment type the following command.

{% highlight bash %}
./peel.sh exp:run wordcount.scale-out wordcount.flink.top10 --run 2
{% endhighlight %}

You can also split the preparation (system setup + data generation), run execution, and finalization (system teardown) phases in three commands as follows.

{% highlight bash %}
./peel.sh exp:setup wordcount.scale-out wordcount.flink.top10
./peel.sh exp:run wordcount.scale-out wordcount.flink.top10 --run 2 --just
./peel.sh exp:teardown wordcount.scale-out wordcount.flink.top10
{% endhighlight %}

### Setting Up / Tearing Down Systems

In addition to the lifecycle-based commands explained above, Peel also offers commands to directly start and stop systems described by system beans.
To start and stop the `flink-0.9.0` system bean, for example, you can use the following command.

{% highlight bash %}
./peel.sh exp:setup flink-0.9.0     # start 
./peel.sh exp:teardown flink-0.9.0  # stop
{% endhighlight %}

Dependent systems are thereby transitively started and stopped (in the above example this includes `hdfs-2.7.1`).
Since the system bean is loaded outside of the context of a particular experiment, it will be configured using the values loaded for the current environment.

## Archiving the Results

Experiment results are maintained in subfolders of `${app.path.results}` and tend to get bigger when you scale out the data and the number of slaves due to the increased number of log files.
To save space and make use of the inherent log file entropy, Peel offers the following commands.

{% highlight bash %}
./peel.sh res:archive wordcount.scale-out
./peel.sh res:extract wordcount.scale-out
{% endhighlight %}

The first command creates a `*.tar.gz` archive for the given experiment path, while the second extracts a previously created archive.
Make sure you archive the results of a suite before you fetch them to your developer machine with `rsync:pull`.