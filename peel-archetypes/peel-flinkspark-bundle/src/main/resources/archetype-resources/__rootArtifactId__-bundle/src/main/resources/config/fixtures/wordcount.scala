#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package config.fixtures

import com.typesafe.config.ConfigFactory
import org.peelframework.core.beans.data.{CopiedDataSet, DataSet, ExperimentOutput, GeneratedDataSet}
import org.peelframework.core.beans.experiment.ExperimentSequence.SimpleParameters
import org.peelframework.core.beans.experiment.{ExperimentSequence, ExperimentSuite}
import org.peelframework.flink.beans.experiment.FlinkExperiment
import org.peelframework.flink.beans.job.FlinkJob
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.peelframework.spark.beans.experiment.SparkExperiment
import org.peelframework.spark.beans.system.Spark
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** `WordCount` experiment fixtures for the '${parentArtifactId}' bundle. */
@Configuration
class wordcount extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Data Generators
  // ---------------------------------------------------

  @Bean(name = Array("datagen.words"))
  def `datagen.words`: FlinkJob = new FlinkJob(
    runner  = ctx.getBean("flink-1.0.3", classOf[Flink]),
    command =
      """
        |-v -c ${package}.datagen.flink.WordGenerator        ${symbol_escape}
        |${symbol_dollar}{app.path.datagens}/${parentArtifactId}-datagens-${version}.jar        ${symbol_escape}
        |${symbol_dollar}{system.default.config.parallelism.total}                           ${symbol_escape}
        |${symbol_dollar}{datagen.tuples.per.task}                                           ${symbol_escape}
        |${symbol_dollar}{datagen.dictionary.dize}                                           ${symbol_escape}
        |${symbol_dollar}{datagen.data-distribution}                                         ${symbol_escape}
        |${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt
      """.stripMargin.trim
  )

  // ---------------------------------------------------
  // Data Sets
  // ---------------------------------------------------

  @Bean(name = Array("dataset.words.static"))
  def `dataset.words.static`: DataSet = new CopiedDataSet(
    src = "${symbol_dollar}{app.path.datasets}/rubbish.txt",
    dst = "${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt",
    fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("dataset.words.generated"))
  def `dataset.words.generated`: DataSet = new GeneratedDataSet(
    src = ctx.getBean("datagen.words", classOf[FlinkJob]),
    dst = "${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt",
    fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("wordcount.output"))
  def `wordcount.output`: ExperimentOutput = new ExperimentOutput(
    path = "${symbol_dollar}{system.hadoop-2.path.output}/wordcount",
    fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("wordcount.default"))
  def `wordcount.default`: ExperimentSuite = {
    val `wordcount.flink.default` = new FlinkExperiment(
      name    = "wordcount.flink.default",
      command =
        """
          |-v -c ${package}.flink.FlinkWC                      ${symbol_escape}
          |${symbol_dollar}{app.path.apps}/${parentArtifactId}-flink-jobs-${version}.jar          ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt                            ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(""),
      runs    = 3,
      runner  = ctx.getBean("flink-1.0.3", classOf[Flink]),
      inputs  = Set(ctx.getBean("dataset.words.static", classOf[DataSet])),
      outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
    )

    val `wordcount.spark.default` = new SparkExperiment(
      name    = "wordcount.spark.default",
      command =
        """
          |--class ${package}.spark.SparkWC                    ${symbol_escape}
          |${symbol_dollar}{app.path.apps}/${parentArtifactId}-spark-jobs-${version}.jar          ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt                            ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(""),
      runs    = 3,
      runner  = ctx.getBean("spark-1.6.0", classOf[Spark]),
      inputs  = Set(ctx.getBean("dataset.words.static", classOf[DataSet])),
      outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
    )

    new ExperimentSuite(Seq(
      `wordcount.flink.default`,
      `wordcount.spark.default`))
  }

  @Bean(name = Array("wordcount.scale-out"))
  def `wordcount.scale-out`: ExperimentSuite = {
    val `wordcount.flink.prototype` = new FlinkExperiment(
      name    = "wordcount.flink.__topXXX__",
      command =
        """
          |-v -c ${package}.flink.FlinkWC                      ${symbol_escape}
          |${symbol_dollar}{app.path.apps}/${parentArtifactId}-flink-jobs-${version}.jar          ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt                            ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(
        """
          |system.default.config.slaves            = ${symbol_dollar}{env.slaves.__topXXX__.hosts}
          |system.default.config.parallelism.total = ${symbol_dollar}{env.slaves.__topXXX__.total.parallelism}
          |datagen.dictionary.dize                 = 10000
          |datagen.tuples.per.task                 = 10000000 ${symbol_pound} ~ 100 MB
          |datagen.data-distribution               = Uniform
        """.stripMargin.trim),
      runs    = 3,
      runner  = ctx.getBean("flink-1.0.3", classOf[Flink]),
      inputs  = Set(ctx.getBean("dataset.words.generated", classOf[DataSet])),
      outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
    )

    val `wordcount.spark.prototype` = new SparkExperiment(
      name    = "wordcount.spark.__topXXX__",
      command =
        """
          |--class ${package}.spark.SparkWC                    ${symbol_escape}
          |${symbol_dollar}{app.path.apps}/${parentArtifactId}-spark-jobs-${version}.jar          ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.input}/rubbish.txt                            ${symbol_escape}
          |${symbol_dollar}{system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(
        """
          |system.default.config.slaves            = ${symbol_dollar}{env.slaves.__topXXX__.hosts}
          |system.default.config.parallelism.total = ${symbol_dollar}{env.slaves.__topXXX__.total.parallelism}
          |datagen.dictionary.dize                 = 10000
          |datagen.tuples.per.task                 = 10000000 ${symbol_pound} ~ 100 MB
          |datagen.data-distribution               = Uniform
        """.stripMargin.trim),
      runs    = 3,
      runner  = ctx.getBean("spark-1.6.0", classOf[Spark]),
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
}