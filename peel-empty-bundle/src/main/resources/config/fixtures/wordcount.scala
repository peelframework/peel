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

/** `WordCount` experiment fixtures for the 'peel-wordcount' bundle. */
@Configuration
class wordcount extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Data Sets
  // ---------------------------------------------------

  @Bean(name = Array("dataset.rubbish"))
  def `dataset.rubbish`: DataSet = new CopiedDataSet(
    src = "${app.path.datasets}/rubbish.txt",
    dst = "${system.hadoop-2.path.input}/rubbish.txt",
    fs  = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
  )

  @Bean(name = Array("wordcount.output"))
  def `wordcount.output`: ExperimentOutput = new ExperimentOutput(
    path = "${system.hadoop-2.path.output}/wordcount",
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
          |-v -c org.peelframework.wordcount.flink.FlinkWC                      \
          |${app.path.apps}/peel-wordcount-flink-jobs-1.0-SNAPSHOT.jar          \
          |${system.hadoop-2.path.input}/rubbish.txt                            \
          |${system.hadoop-2.path.output}/wordcount
        """.stripMargin.trim,
      config  = ConfigFactory.parseString(""),
      runs    = 3,
      runner  = ctx.getBean("flink-0.9.0", classOf[Flink]),
      inputs  = Set(ctx.getBean("dataset.rubbish", classOf[DataSet])),
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
      runner  = ctx.getBean("spark-1.4.0", classOf[Spark]),
      inputs  = Set(ctx.getBean("dataset.rubbish", classOf[DataSet])),
      outputs = Set(ctx.getBean("wordcount.output", classOf[ExperimentOutput]))
    )

    new ExperimentSuite(Seq(
      `wordcount.flink.default`,
      `wordcount.spark.default`))
  }
}