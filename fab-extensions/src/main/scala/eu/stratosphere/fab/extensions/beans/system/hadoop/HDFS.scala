package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.System
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import com.typesafe.config.{ConfigFactory, Config}


class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set()) extends System(lifespan, dependencies) {

  final val config: Config = ConfigFactory.load("hdfs.v1.conf")
  // TODO add fallback for own configs or merge configs

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
    configure(config)
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
  }

  def update(): Unit = {
    logger.info("Updating " + toString + "...")
    tearDown()
    setUp()
  }

  def configure(conf: Config) = {
    scala.xml.XML.save("core-site.xml", coreSite(conf.getString("core-site.name"), conf.getString("core-site.value")))
  }

  val coreSite = { (name: String, value: String) =>
    <configuration>
      <property>
        <name>{name}</name>
        <value>{value}</value>
      </property>
    </configuration>
  }

  override def toString = "HDFS v1"

}
