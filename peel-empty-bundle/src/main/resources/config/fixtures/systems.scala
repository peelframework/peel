package config.fixtures

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.peelframework.spark.beans.system.Spark
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** System beans for the 'peel-wordcount' bundle. */
@Configuration
class systems extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Systems
  // ---------------------------------------------------

  @Bean(name = Array("flink-0.9.0"))
  def `flink-0.9.0`: Flink = new Flink(
    version      = "0.9.0",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(ctx.getBean("hdfs-2.7.1", classOf[HDFS2])),
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-1.4.0"))
  def `spark-1.4.0`: Spark = new Spark(
    version      = "1.4.0",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(ctx.getBean("hdfs-2.7.1", classOf[HDFS2])),
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )
}