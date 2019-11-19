---
layout: manual
title: Experiments Definitions
date: 2015-07-07 10:00:00
nav: [ manual, experiments-definitions ]
---

# {{ page.title }}

Experiments are defined in Peel using a Spring [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection) container as a set of inter-connected beans.
In this section we present the available bean types and illustrate how they can be defined based on our `peel-wordcount` example.

Beans definitions can be done either in [XML](http://docs.spring.io/autorepo/docs/spring/current/spring-framework-reference/html/expressions.html#expressions-beandef-xml-based) or in [annotated Scala classes](http://docs.spring.io/autorepo/docs/spring/current/spring-framework-reference/html/expressions.html#expressions-beandef-annotation-based). 
We recommend the first for small experiments and the latter for more complex fixtures.

The entry point of your definitions is either

* [`config/experiments.xml` (for XML-based definitions)](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config/experiments.xml), or 
* [`config/experiments.scala` (for Scala-based definitions)](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config/experiments.scala).  

Scala files are kept as source and compiled lazily in order to ease late modifications on the server side.
If you change something in a Scala source, make sure to delete the corresponding `*.class` before you run Peel in order to reflect the source changes.

If both files exist, XML-based definitions have precedence and the `*.scala` files are silently ignored. 

## Domain Model

The beans required to define an experiment realize the system experiments domain introduced in the [Motivation]({{ site.baseurl }}/manual/motivation.html) section.
A UML diagram of the domain model can be seen below.

<div class="row">
    <figure class="large-10 large-centered medium-11 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/domain_model_uml.svg" title="Peel domain model: UML class diagram" alt="Peel domain model: UML class diagram" /><br />
        <figcaption>Peel domain model: UML class diagram</figcaption>
    </figure>
</div>

### Experiment

The central class in this model is [*Experiment*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/experiment/Experiment.scala). 
It specifies the following properties:

* the experiment *name*;
* the *command* that executes the experiment application;
* the number of *runs* (repetitions) the experiment is executed; 
* the *inputs* required and *outputs* produced by each run;
* the *runner* system that carries the execution; 
* other *systems*, upon which the execution of the experiment depends;
* the experiment-specific environment *config*, as discussed in the ["Configuration Layers"]({{ site.baseurl }}/manual/environment-configurations.html#configuration-layers) section.

### System

The second important class in the model is [*System*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/system/System.scala). 
It specifies the following properties:

* the system *name*, usually fixed per *System* implementation, e.g. `flink` for [the *Flink* system](https://github.com/stratosphere/peel/blob/master/peel-extensions/src/main/scala/org/peelframework/flink/beans/system/Flink.scala) or `spark` for [the *Spark* system](https://github.com/stratosphere/peel/blob/master/peel-extensions/src/main/scala/org/peelframework/spark/beans/system/Spark.scala);
* the system *version*, e.g. `0.9.0` for Flink or `1.3.1` for Spark;
* a *configKey* under which config parameters will be located in the environment configuration, usually the same as the system name;
* a [*Lifespan*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/system/Lifespan.scala) value (one of *Provided*, *Suite*, *Experiment*, or *Run*) which tells Peel when to start and stop the system;
* a list of systems upon which the current system depends.

As a restriction, a dependent system must never have a wider *Lifespan* value then its dependencies.

### Experiment Suite

A series of related experiment beans are organized in an [*ExperimentSuite*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/experiment/ExperimentSuite.scala). 
Typically, the experiments within a suite should be closely related and a used to characterize a certain behavior.
For example, in our `peel-wordcount` bundle we want to define a `wordcount.scale-out` suite which consists of the following six experiment beans:

* `wordcount.flink.top05` - execute WordCount on Flink using 5 workers,
* `wordcount.flink.top10` - execute WordCount on Flink using 10 workers,
* `wordcount.flink.top20` - execute WordCount on Flink using 20 workers,
* `wordcount.spark.top05` - execute WordCount on Spark using 5 workers,
* `wordcount.spark.top10` - execute WordCount on Spark using 10 workers,
* `wordcount.spark.top20` - execute WordCount on Spark using 20 workers.

### Input and Output Data

Experiments typically depend on some kind of input data, represented as abstract [*DataSet*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/data/DataSet.scala) elements associated with a particular [*FileSystem*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/system/FileSystem.scala) in our model. 
The following types are currently supported:

* [*CopiedDataSet*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/data/CopiedDataSet.scala) - used for static data copied into the target *FileSystem*;
* [*GeneratedDataSet*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/data/GeneratedDataSet.scala) - used for data generated by a Job into the target *FileSystem*.

In addition, each *Experiment* bean is associated with an [*ExperimentOutput*](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/data/ExperimentOutput.scala) which describes the paths with data written by the experiment workload application. Peel uses this meta-information to clean those paths upon execution.

## Dependency Graph

An experiment suite induces a dependency graph based on the following rules:

* An *ExperimentSuite* depends on its experiments;
* An *Experiment* depends on its *runner*, *systems*, and *inputs*;
* A *DataSet* depends on its *FileSystem*;
* A *System* depends on its *dependencies*.

This dependency graph is used to determine which systems need to be running at various points in the lifecycle of an experiment. As a restriction, the dependency graph cannot be cyclic.

## Supported Systems

The `peel-extensions` module ships with several *System* implementations. The following systems are defined in the [peel-extensions.xml](https://github.com/stratosphere/peel/blob/master/peel-extensions/src/main/resources/peel-extensions.xml) and can be used out of the box.

| System           | Version        | System bean ID  |
| ---------------- | ----------------------------------
| HDFS             | 1.2.1          | hdfs-1.2.1      |
| HDFS             | 2.4.1          | hdfs-2.4.1      |
| HDFS             | 2.7.1          | hdfs-2.7.1      |
| HDFS             | 3.1.1          | hdfs-3.1.1      |
| Yarn             | 3.1.1          | yarn-3.1.1      |
| Flink            | 0.8.0          | flink-0.8.0     |
| Flink            | 0.8.1          | flink-0.8.1     |
| Flink            | 0.9.0          | flink-0.9.0     |
| Flink            | 0.10.0         | flink-0.10.0    |
| Flink            | 0.10.1         | flink-0.10.1    |
| Flink            | 0.10.2         | flink-0.10.2    |
| Flink            | 1.0.0          | flink-1.0.0     |
| Flink            | 1.0.1          | flink-1.0.1     |
| Flink            | 1.0.2          | flink-1.0.2     |
| Flink            | 1.0.3          | flink-1.0.3     |
| Flink            | 1.1.0          | flink-1.1.0     |
| Flink            | 1.1.1          | flink-1.1.1     |
| Flink            | 1.1.2          | flink-1.1.2     |
| Flink            | 1.1.3          | flink-1.1.3     |
| Flink            | 1.1.4          | flink-1.1.4     |
| Flink            | 1.2.0          | flink-1.2.0     |
| Flink Standalone Cluster | 1.7.0  | flink-1.7.0     |
| Flink Standalone Cluster | 1.7.2  | flink-1.7.2     |
| Flink Yarn Session | 1.7.0        | flink-yarn-1.7.0 |
| Flink Yarn Session | 1.7.2        | flink-yarn-1.7.2 |
| MapReduce        | 1.2.1          | mapred-1.2.1    |
| MapReduce        | 2.4.1          | mapred-2.4.1    |
| Spark            | 1.3.1          | spark-1.3.1     |
| Spark            | 1.4.0          | spark-1.4.0     |
| Spark            | 1.4.1          | spark-1.4.1     |
| Spark            | 1.5.1          | spark-1.5.1     |
| Spark            | 1.5.2          | spark-1.5.2     |
| Spark            | 1.6.0          | spark-1.6.0     |
| Spark            | 1.6.2          | spark-1.6.2     |
| Spark            | 2.0.0          | spark-2.0.0     |
| Spark            | 2.0.1          | spark-2.0.1     |
| Spark            | 2.0.2          | spark-2.0.2     |
| Spark            | 2.1.0          | spark-2.1.0     |
| Zookeeper        | 3.4.5          | zookeeper-3.4.5 |
| Dstat            | 0.7.2          | dstat-0.7.2     |

Each system bean has a [a default configuration entry](https://github.com/stratosphere/peel/tree/master/peel-extensions/src/main/resources) with name `reference.${systemID}.conf` which sets values suitable for running experiments on your local machine. Browse the corresponding [*peel-extensions*](https://github.com/stratosphere/peel/tree/master/peel-extensions/src/main/scala/org/peelframework/extensions) packages to see what other beans are implemented and available for each system.

If you want to add support for another system, use one of the existing implementations as a starting point. [Contributions are welcome](https://github.com/stratosphere/peel/pulls)!

## Example

Let us again take a look at the layout of the basic two experiments in the `peel-wordcount` bundle.

<div class="row">
    <figure class="large-7 large-centered medium-8 medium-centered small-10 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc.svg" title="Environment layout for 'WordCount: Spark vs. Flink'" alt="Environment layout for 'WordCount: Spark vs. Flink'" /><br />
        <figcaption>Environment layout for "WordCount: Spark vs. Flink"</figcaption>
    </figure>
</div>

From that figure, we can infer the following basic elements of our domain model.

* *System* beans - *Flink*, *Spark*, and their common dependency *HDFS*.
* A *DataSet* bean that represents the WordCount job input.
* An *ExperimentOutput* bean that represents the WordCount job output.
* An *Experiment* bean for the *FlinkWC* job which depends on the *Flink* system.
* An *Experiment* bean for the *SparkWC* job which depends on the *Spark* system.

We will now go through each of the above elements and show the corresponding bean definitions using XML and Scala-based Spring syntax.

### Systems

We start with the definition of the systems. 
For our setup, we want to override the `flink-0.9.0` and `spark-1.3.1` beans so they depend on `hdfs-2.7.1` and reuse `hdfs-2.7.1` unmodified.

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex01-xml">XML</a></li>
  <li class="tab-title"><a href="#ex01-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex01-xml">
{% highlight xml %}
<bean id="flink-0.9.0" class="org.peelframework.flink.beans.system.Flink" parent="system">
    <constructor-arg name="version" value="0.9.0"/>
    <constructor-arg name="configKey" value="flink" />
    <constructor-arg name="lifespan" value="EXPERIMENT"/>
    <constructor-arg name="dependencies">
        <set value-type="org.peelframework.core.beans.system.System">
            <ref bean="hdfs-2.7.1"/>
        </set>
    </constructor-arg>
</bean>

<bean id="spark-1.4.0" class="org.peelframework.spark.beans.system.Spark" parent="system">
    <constructor-arg name="version" value="1.4.0"/>
    <constructor-arg name="configKey" value="spark" />
    <constructor-arg name="lifespan" value="EXPERIMENT"/>
    <constructor-arg name="dependencies">
        <set value-type="org.peelframework.core.beans.system.System">
            <ref bean="hdfs-2.7.1"/>
        </set>
    </constructor-arg>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex01-scala">
{% highlight scala %}
@Bean(name = Array("flink-0.9.0"))
def `flink-0.9.0`: Flink = new Flink(
  version      = "0.9.0",
  configKey    = "flink",
  lifespan     = Lifespan.EXPERIMENT,
  dependencies = Set(ctx.getBean("hdfs-2.7.1", classOf[HDFS2])),
  mc           = ctx.getBean(classOf[Mustache.Compiler])
)

@Bean(name = Array("spark-1.3.1"))
def `spark-1.3.1`: Spark = new Spark(
  version      = "1.3.1",
  configKey    = "spark",
  lifespan     = Lifespan.EXPERIMENT,
  dependencies = Set(ctx.getBean("hdfs-2.7.1", classOf[HDFS2])),
  mc           = ctx.getBean(classOf[Mustache.Compiler])
)
{% endhighlight %}
</div>

</div>

For better structuring and maintenance, the definitions are factored out in dedicated files 

* [`config/fixtures/systems.xml`](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config/fixtures/systems.xml) and 
* [`config/fixtures/systems.scala`](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config/fixtures/systems.scala).

### Input and Output Data

For the *DataSet* that represents the WordCount job input, we have two options - copy a static dataset shipped as part of the bundle or generate the data on the fly.

For the first option, we first place the text file under `peel-wordcount-bundle/src/main/resources/datasets`, which upon building the bundle binaries is copied under `${app.path.datasets}`.
Upon that, we can define the corresponding *CopiedDataSet* bean as follows.

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex02-xml">XML</a></li>
  <li class="tab-title"><a href="#ex02-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex02-xml">
{% highlight xml %}
<bean id="dataset.words.static" class="org.peelframework.core.beans.data.CopiedDataSet">
    <constructor-arg name="src" value="${app.path.datasets}/rubbish.txt"/>
    <constructor-arg name="dst" value="${system.hadoop-2.path.input}/rubbish.txt"/>
    <constructor-arg name="fs" ref="hdfs-2.7.1"/>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex02-scala">
{% highlight scala %}
@Bean(name = Array("dataset.words.static"))
def `dataset.words.static`: DataSet = new CopiedDataSet(
  src = "${app.path.datasets}/rubbish.txt",
  dst = "${system.hadoop-2.path.input}/rubbish.txt",
  fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
)
{% endhighlight %}
</div>

</div>

The bean then delegates to the underlying *FileSystem* (in this case `hdfs-2.7.1`) in order to copy the file from the given `src` to the given `dst`.

For the second option, we first add [a WordGenerator Flink job](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-datagens/src/main/scala/org/peelframework/wordcount/datagen/flink/WordGenerator.scala) to the `peel-wordcount-datagens` project and configure a corresponding [*FlinkJob*](https://github.com/stratosphere/peel/blob/master/peel-extensions/src/main/scala/org/peelframework/extensions/flink/beans/job/FlinkJob.scala) bean. 

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex03-xml">XML</a></li>
  <li class="tab-title"><a href="#ex03-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex03-xml">
{% highlight xml %}
<bean id="datagen.words" class="org.peelframework.flink.beans.job.FlinkJob">
    <constructor-arg name="runner" ref="flink-0.9.0"/>
    <constructor-arg name="command">
        <value><![CDATA[
          -v -c org.peelframework.wordcount.datagen.flink.WordGenerator        \
          ${app.path.datagens}/peel-wordcount-datagens-1.0-SNAPSHOT.jar        \
          ${datagen.dictionary.dize}                                           \
          ${system.default.config.parallelism.total}                           \
          ${datagen.tuples.per.task}                                           \
          ${system.hadoop-2.path.input}/rubbish.txt
        ]]>
        </value>
    </constructor-arg>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex03-scala">
{% highlight scala %}
@Bean(name = Array("dataset.words"))
def `datagen.words`: FlinkJob = new FlinkJob(
  runner  = ctx.getBean("flink-0.9.0", classOf[Flink]),
  command = """
              | -v -c org.peelframework.wordcount.datagen.flink.WordGenerator        \
              | ${app.path.datagens}/peel-wordcount-datagens-1.0-SNAPSHOT.jar        \
              | ${datagen.dictionary.dize}                                           \
              | ${system.default.config.parallelism.total}                           \
              | ${datagen.tuples.per.task}                                           \
              | ${system.hadoop-2.path.input}/rubbish.txt
            """.stripMargin.trim
  )
{% endhighlight %}
</div>

</div>

The bean relies on the `flink-0.9.0` system (which is [provided out of the box as explained above](#supported-systems)) and runs the given `command`.

{% highlight bash %}
/flink-home/bin/flink run ${command}
{% endhighlight %}

The values for the parameters in the command string are thereby taken and substituted based on the environment configuration of the enclosing experiment bean.
This allows us to vary parameters like the DOP and the size of the dictionary per experiment.
The final result is written in `${system.hadoop-2.path.input}/rubbish.txt`.

With that, we can configure a *DataSet* bean that runs the `datagen.words` job on demand to generate the words file.

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex04-xml">XML</a></li>
  <li class="tab-title"><a href="#ex04-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex04-xml">
{% highlight xml %}
<bean id="dataset.words.generated" class="org.peelframework.core.beans.data.GeneratedDataSet">
    <constructor-arg name="src" ref="datagen.words"/>
    <constructor-arg name="dst" value="${system.hadoop-2.path.input}/rubbish.txt"/>
    <constructor-arg name="fs" ref="hdfs-2.7.1"/>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex04-scala">
{% highlight scala %}
@Bean(name = Array("dataset.words.generated"))
def `dataset.words.generated`: DataSet = new GeneratedDataSet(
  src = ctx.getBean("datagen.words", classOf[FlinkJob]),
  dst = "${system.hadoop-2.path.input}/rubbish.txt",
  fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
)
{% endhighlight %}
</div>

</div>

We also add an *ExperimentOutput* bean for the path where the WordCount jobs will write their result.

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex05-xml">XML</a></li>
  <li class="tab-title"><a href="#ex05-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex05-xml">
{% highlight xml %}
<bean id="wordcount.output" class="org.peelframework.core.beans.data.ExperimentOutput">
    <constructor-arg name="path" value="${system.hadoop-2.path.output}/wordcount"/>
    <constructor-arg name="fs" ref="hdfs-2.7.1"/>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex05-scala">
{% highlight scala %}
@Bean(name = Array("wordcount.output"))
def `wordcount.output`: ExperimentOutput = new ExperimentOutput(
  path = "${system.hadoop-2.path.output}/wordcount",
  fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
)
{% endhighlight %}
</div>

</div>

### Experiments and Experiment Suites

With the above beans we can define the two experiments that respectively run [the FlinkWc job](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-flink-jobs/src/main/scala/org/peelframework/wordcount/flink/FlinkWC.scala) on Flink and [the SparkWC job](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-spark-jobs/src/main/scala/org/peelframework/wordcount/spark/SparkWC.scala) on Spark.

If we leave the `config` parameters empty, the experiments will be executed using the default configuration for the current environment.
We wrap this variant in a `word-count.default` bundle.

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex06-xml">XML</a></li>
  <li class="tab-title"><a href="#ex06-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex06-xml">
{% highlight xml %}
<bean id="wordcount.default" class="org.peelframework.core.beans.experiment.ExperimentSuite">
    <constructor-arg name="experiments">
        <list value-type="org.peelframework.core.beans.experiment.Experiment">
            <bean id="experiment.flink.wordcount"  class="org.peelframework.flink.beans.experiment.FlinkExperiment">
                <constructor-arg name="name" value="wordcount.flink"/>
                <constructor-arg name="runs" value="3"/>
                <constructor-arg name="command">
                    <value><![CDATA[
                      -v -c org.peelframework.wordcount.flink.FlinkWC                      \
                      ${app.path.apps}/peel-wordcount-flink-jobs-1.0-SNAPSHOT.jar          \
                      ${system.hadoop-2.path.input}/rubbish.txt                            \
                      ${system.hadoop-2.path.output}/wordcount
                    ]]></value>
                </constructor-arg>
                <constructor-arg name="config" value=""/>
                <constructor-arg name="runner" ref="flink-0.9.0"/>
                <constructor-arg name="outputs">
                    <set value-type="org.peelframework.core.beans.data.ExperimentOutput">
                        <ref bean="wordcount.output"/>
                    </set>
                </constructor-arg>
                <constructor-arg name="inputs">
                    <set>
                        <ref bean="dataset.words.static" />
                    </set>
                </constructor-arg>
            </bean>
            <bean id="experiment.spark.wordcount" class="org.peelframework.spark.beans.experiment.SparkExperiment" abstract="true">
                <constructor-arg name="name" value="wordcount.spark"/>
                <constructor-arg name="runs" value="3"/>
                <constructor-arg name="command">
                    <value><![CDATA[
                      --class org.peelframework.wordcount.spark.SparkWC                    \
                      ${app.path.apps}/peel-wordcount-spark-jobs-1.0-SNAPSHOT.jar          \
                      ${system.hadoop-2.path.input}/rubbish.txt                            \
                      ${system.hadoop-2.path.output}/wordcount
                    ]]></value>
                </constructor-arg>
                <constructor-arg name="config" value=""/>
                <constructor-arg name="outputs">
                    <set value-type="org.peelframework.core.beans.data.ExperimentOutput">
                        <ref bean="wordcount.output"/>
                    </set>
                </constructor-arg>
                <constructor-arg name="inputs">
                    <set>
                        <ref bean="dataset.words.static" />
                    </set>
                </constructor-arg>
                <constructor-arg name="runner" ref="spark-1.4.0"/>
            </bean>
        </list>
    </constructor-arg>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex06-scala">
{% highlight scala %}
@Bean(name = Array("wordcount.default"))
def `wordcount.default`: ExperimentSuite = {
  val `wordcount.flink.default` = new FlinkExperiment(
    name    = "wordcount.flink.default",
    command =
      """
        |-v -c org.peelframework.wordcount.flink.FlinkWC                      \
        |${app.path.apps}/peel-wordcount-flink-jobs-1.0-SNAPSHOT.jar          \
        |${system.hadoop-2.path.input}/rubbish.txt                            \
        |${system.hadoop-2.path.output}/wordcount
      """.stripMargin.trim,
    config  = ConfigFactory.parseString(""),
    runs    = 3,
    runner  = ctx.getBean("flink-0.9.0", classOf[Flink]),
    inputs  = Set(ctx.getBean("dataset.words.static", classOf[DataSet])),
    outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
  )

  val `wordcount.spark.default` = new SparkExperiment(
    name    = "wordcount.spark.default",
    command =
      """
        |--class org.peelframework.wordcount.spark.SparkWC                    \
        |${app.path.apps}/peel-wordcount-spark-jobs-1.0-SNAPSHOT.jar          \
        |${system.hadoop-2.path.input}/rubbish.txt                            \
        |${system.hadoop-2.path.output}/wordcount
      """.stripMargin.trim,
    config  = ConfigFactory.parseString(""),
    runs    = 3,
    runner  = ctx.getBean("spark-1.3.1", classOf[Spark]),
    inputs  = Set(ctx.getBean("dataset.words.static", classOf[DataSet])),
    outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
  )

  new ExperimentSuite(Seq(
    `wordcount.flink.default`,
    `wordcount.spark.default`))
}
{% endhighlight %}
</div>

</div>

Now let us consider a situation where we want to execute a series of experiments with a varying parameter, e.g. varying both the input size and the number of workes in a `wordcount.scale-out` suite.
Here is again the desired environment layout.

<div class="row">
    <figure class="large-9 large-centered medium-10 medium-centered small-12 small-centered columns">
        <img src="{{ site.baseurl }}/img/env_wc_series.svg" title="Environment layouts for 'Weak Scale-Out - Spark vs. Flink'" alt="Environment layouts for 'Weak Scale-Out - Spark vs. Flink'" /><br />
        <figcaption>Environment layouts for "Weak Scale-Out - Spark vs. Flink"</figcaption>
    </figure>
</div>

In order define this layout, we need to create a suite with the following six experiments:
 
* `wordcount.flink.top05` - execute WordCount on Flink using 5 workers,
* `wordcount.flink.top10` - execute WordCount on Flink using 10 workers,
* `wordcount.flink.top20` - execute WordCount on Flink using 20 workers,
* `wordcount.spark.top05` - execute WordCount on Spark using 5 workers,
* `wordcount.spark.top10` - execute WordCount on Spark using 10 workers,
* `wordcount.spark.top20` - execute WordCount on Spark using 20 workers.

For each pair of Flink and Spark experiments, we vary the number of workers over 5, 10, and 20 respectively, as well as the amount of data processed per task.
This requires setting proper values to the following two config parameters:

* `system.default.config.slaves` - A list of hosts to be used as worker nodes.
* `system.default.config.parallelism.total` - The total number of workers (i.e., the DOP).

The values from the `system.default` namespace are used per default in all other systems, so we don't need to explicitly set them for HDFS, Flink and Spark.

In order to assign the correct list, we make use of the predefined [hosts.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/hosts.conf) file for the ACME cluster. The command that generated this file is as follows:

{% highlight scala %}
./peel.sh hosts:generate ./config/acme-master/hosts.conf  \
          --masters "acme-master"                         \
          --slaves-pattern "acme-%03d"                    \
          --slaves-include "[1,40]"                       \
          --parallelism 32                                \
          --memory 33554432                               \
          --unit 5
{% endhighlight %}

Because the unit is set to 5 and the number of workers is 40 (`acme-001` through `acme-040`), the [hosts.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/hosts.conf) will contain the following config entries

* `env.slaves.all` - configuration using all workers.
* `env.slaves.top005` - using the top 5 workers (`acme-001` through `acme-005`)
* `env.slaves.top010` - using the top 10 workers (`acme-001` through `acme-010`)
* `env.slaves.top020` - using the top 20 workers (`acme-001` through `acme-020`)
* `env.slaves.top040` - using the top 40 workers (`acme-001` through `acme-040`)

Per default, we have the following config values set in ACME's [application.conf](https://github.com/stratosphere/peelconfig.acme/blob/master/application.conf):

{% highlight docker %}
system {
    default {
        config {
            masters = ${env.masters}
            slaves = ${env.slaves.all.hosts}
            parallelism.per-node = ${env.per-node.parallelism}
            parallelism.total = ${env.slaves.all.total.parallelism}
        }
    }
}
{% endhighlight %}

Recall that the `parallelism.total` parameter [is used in the command of data generation job](#input-and-output-data-1).
Based on this insight, we know that for the weak scale-out suite we need to parameterize the environment configuration per experiment as follows:

{% highlight docker %}
system {
    default {
        config {
            slaves = ${env.slaves.__topXXX__.hosts}
            parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
        }
    }
}
{% endhighlight %}

Where `__topXXX__` is a placeholder that iterates over the following values: `top005`, `top010`, and `top020`. 
In order to minimize the boilerplate code in a situation like this, Peel offers an [ExperimentSequence](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/experiment/ExperimentSequence.scala) factory.
The factory receives a sequence of parameter maps and instantiates sequence of experiment prototypes for each item in the list.
One can use the following `Parameters` factories.

* [SimpleParameters](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/experiment/ExperimentSequence.scala#L100) - Constructs a singleton map for each one.
* [ZippedParameters](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/experiment/ExperimentSequence.scala#L118) - Constructs a map from a zipped sequence of parameter sequences.
* [CrossedParameters](https://github.com/stratosphere/peel/blob/master/peel-core/src/main/scala/org/peelframework/core/beans/experiment/ExperimentSequence.scala#L138) - Constructs a map for each combination of parameter values.

As in our situation we only have one varying parameter (`topXXX`), we can use SimpleParameters.
The code snippet looks as follows

<ul class="tabs" data-tab>
  <li class="tab-title active"><a href="#ex07-xml">XML</a></li>
  <li class="tab-title"><a href="#ex07-scala">Scala</a></li>
</ul>

<div class="tabs-content">

<div class="content active" id="ex07-xml">
{% highlight xml %}
<bean id="wordcount.scale-out" class="org.peelframework.core.beans.experiment.ExperimentSuite">
    <constructor-arg name="experiments">
        <bean class="org.peelframework.core.beans.experiment.ExperimentSequence">
            <constructor-arg name="paramName" value="topXXX"/>
            <constructor-arg name="paramVals">
                <list>
                    <value>top005</value>
                    <value>top010</value>
                    <value>top020</value>
                </list>
            </constructor-arg>
            <constructor-arg name="prototypes">
                <list value-type="org.peelframework.core.beans.experiment.Experiment">
                    <bean parent="experiment.flink.wordcount">
                        <constructor-arg name="name" value="wordcount.flink.__topXXX__"/>
                        <constructor-arg name="inputs">
                            <set>
                                <ref bean="dataset.words.generated" />
                            </set>
                        </constructor-arg>
                        <constructor-arg name="config">
                            <value><![CDATA[
                                system.default.config.slaves            = ${env.slaves.__topXXX__.hosts}
                                system.default.config.parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
                                datagen.dictionary.dize                 = 10000
                                datagen.tuples.per.task                 = 10000000 # ~ 100 MB
                            ]]></value>
                        </constructor-arg>
                    </bean>
                    <bean parent="experiment.spark.wordcount">
                        <constructor-arg name="name" value="wordcount.spark.__topXXX__"/>
                        <constructor-arg name="inputs">
                            <set>
                                <ref bean="dataset.words.generated" />
                            </set>
                        </constructor-arg>
                        <constructor-arg name="config">
                            <value><![CDATA[
                              system.default.config.slaves            = ${env.slaves.__topXXX__.hosts}
                              system.default.config.parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
                              datagen.dictionary.dize                 = 10000
                              datagen.tuples.per.task                 = 10000000 # ~ 100 MB
                            ]]></value>
                        </constructor-arg>
                    </bean>
                </list>
            </constructor-arg>
        </bean>
    </constructor-arg>
</bean>
{% endhighlight %}
</div>

<div class="content" id="ex07-scala">
{% highlight scala %}
@Bean(name = Array("wordcount.scale-out"))
def `wordcount.scale-out`: ExperimentSuite = {
  val `wordcount.flink.prototype` = new FlinkExperiment(
    name    = "wordcount.flink.__topXXX__",
    command =
      """
        |-v -c org.peelframework.wordcount.flink.FlinkWC                      \
        |${app.path.apps}/peel-wordcount-flink-jobs-1.0-SNAPSHOT.jar          \
        |${system.hadoop-2.path.input}/rubbish.txt                            \
        |${system.hadoop-2.path.output}/wordcount
      """.stripMargin.trim,
    config  = ConfigFactory.parseString(
      """
        |system.default.config.slaves            = ${env.slaves.__topXXX__.hosts}
        |system.default.config.parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
        |datagen.dictionary.dize                 = 10000
        |datagen.tuples.per.task                 = 10000000 # ~ 100 MB
      """.stripMargin.trim),
    runs    = 3,
    runner  = ctx.getBean("flink-0.9.0", classOf[Flink]),
    inputs  = Set(ctx.getBean("dataset.words.generated", classOf[DataSet])),
    outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
  )

  val `wordcount.spark.prototype` = new SparkExperiment(
    name    = "wordcount.spark.__topXXX__",
    command =
      """
        |--class org.peelframework.wordcount.spark.SparkWC                    \
        |${app.path.apps}/peel-wordcount-spark-jobs-1.0-SNAPSHOT.jar          \
        |${system.hadoop-2.path.input}/rubbish.txt                            \
        |${system.hadoop-2.path.output}/wordcount
      """.stripMargin.trim,
    config  = ConfigFactory.parseString(
      """
        |system.default.config.slaves            = ${env.slaves.__topXXX__.hosts}
        |system.default.config.parallelism.total = ${env.slaves.__topXXX__.total.parallelism}
        |datagen.dictionary.dize                 = 10000
        |datagen.tuples.per.task                 = 10000000 # ~ 100 MB
      """.stripMargin.trim),
    runs    = 3,
    runner  = ctx.getBean("spark-1.3.1", classOf[Spark]),
    inputs  = Set(ctx.getBean("dataset.words.generated", classOf[DataSet])),
    outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
  )

  new ExperimentSuite(
    new ExperimentSequence(
      parameters = new SimpleParameters(
        paramName = "topXXX",
        paramVals = Seq("top005", "top010", "top020")),
      prototypes = Seq(
        `wordcount.flink.prototype`,
        `wordcount.spark.prototype`)))
}
{% endhighlight %}
</div>

</div>

Similar to the *System* beans, the definitions of the *DataSet*, *Experiment*, and *ExperimentSuite* beans for the WordCount experiment are factored out in dedicated files
 
* [`config/fixtures/wordcount.xml`](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config/fixtures/wordcount.xml) and  
* [`config/fixtures/wordcount.scala`](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config/fixtures/wordcount.scala).   

The full configuration as initialized in your `peel-wordcount` bundle [can be found in the GitHub repository](https://github.com/stratosphere/peel-wordcount/blob/master/peel-wordcount-bundle/src/main/resources/config).
