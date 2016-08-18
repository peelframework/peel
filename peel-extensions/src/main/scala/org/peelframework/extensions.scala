/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework

import com.samskivert.mustache.Mustache
import org.peelframework.core.beans.system.Lifespan
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.{HDFS2, Yarn}
import org.peelframework.spark.beans.system.Spark
import org.peelframework.zookeeper.beans.system.Zookeeper
import org.springframework.context.annotation.{Bean, Configuration}
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

@Configuration
class extensions extends ApplicationContextAware {

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Systems
  // ---------------------------------------------------

  // Zookeeper

  @Bean(name = Array("zookeeper-3.4.5"))
  def `zookeeper-3.4.5`: Zookeeper = new Zookeeper(
    version      = "3.4.5",
    configKey    = "zookeeper",
    lifespan     = Lifespan.SUITE,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  // HDFS

  @Bean(name = Array("hdfs-1.2.1"))
  def `hdfs-1.2.1`: HDFS2 = new HDFS2(
    version      = "1.2.1",
    configKey    = "hadoop-1",
    lifespan     = Lifespan.SUITE,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("hdfs-2.4.1"))
  def `hdfs-2.4.1`: HDFS2 = new HDFS2(
    version      = "2.4.1",
    configKey    = "hadoop-2",
    lifespan     = Lifespan.SUITE,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("hdfs-2.7.1"))
  def `hdfs-2.7.1`: HDFS2 = new HDFS2(
    version      = "2.7.1",
    configKey    = "hadoop-2",
    lifespan     = Lifespan.SUITE,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  // Yarn

  @Bean(name = Array("yarn-2.4.1"))
  def `yarn-2.4.1`: Yarn = new Yarn(
    version      = "2.4.1",
    configKey    = "hadoop-2",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("yarn-2.7.1"))
  def `yarn-2.7.1`: Yarn = new Yarn(
    version      = "2.7.1",
    configKey    = "hadoop-2",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  // Flink

  @Bean(name = Array("flink-0.8.0"))
  def `flink-0.8.0`: Flink = new Flink(
    version      = "0.8.0",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-0.8.1"))
  def `flink-0.8.1`: Flink = new Flink(
    version      = "0.8.1",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-0.9.0"))
  def `flink-0.9.0`: Flink = new Flink(
    version      = "0.9.0",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-0.10.0"))
  def `flink-0.10.0`: Flink = new Flink(
    version      = "0.10.0",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-0.10.1"))
  def `flink-0.10.1`: Flink = new Flink(
    version      = "0.10.1",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-0.10.2"))
  def `flink-0.10.2`: Flink = new Flink(
    version      = "0.10.2",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-1.0.0"))
  def `flink-1.0.0`: Flink = new Flink(
    version      = "1.0.0",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-1.0.1"))
  def `flink-1.0.1`: Flink = new Flink(
    version      = "1.0.1",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-1.0.2"))
  def `flink-1.0.2`: Flink = new Flink(
    version      = "1.0.2",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("flink-1.0.3"))
  def `flink-1.0.3`: Flink = new Flink(
    version      = "1.0.3",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  // Spark

  @Bean(name = Array("spark-1.3.1"))
  def `spark-1.3.1`: Spark = new Spark(
    version      = "1.3.1",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-1.4.0"))
  def `spark-1.4.0`: Spark = new Spark(
    version      = "1.4.0",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-1.5.1"))
  def `spark-1.5.1`: Spark = new Spark(
    version      = "1.5.1",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-1.5.2"))
  def `spark-1.5.2`: Spark = new Spark(
    version      = "1.5.2",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-1.6.0"))
  def `spark-1.6.0`: Spark = new Spark(
    version      = "1.6.0",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-1.6.2"))
  def `spark-1.6.2`: Spark = new Spark(
    version      = "1.6.2",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  @Bean(name = Array("spark-2.0.0"))
  def `spark-2.0.0`: Spark = new Spark(
    version      = "2.0.0",
    configKey    = "spark",
    lifespan     = Lifespan.EXPERIMENT,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )

  // DStat

  @Bean(name = Array("dstat-0.7.2"))
  def `dstat-0.7.2`: Dstat = new Dstat(
    version      = "0.7.2",
    configKey    = "dstat",
    lifespan     = Lifespan.RUN,
    mc           = ctx.getBean(classOf[Mustache.Compiler])
  )
}
