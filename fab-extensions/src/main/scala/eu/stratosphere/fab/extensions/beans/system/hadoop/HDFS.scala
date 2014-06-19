package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.System
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConverters._
import java.io.File
import eu.stratosphere.fab.core.beans.Shell


class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set()) extends System(lifespan, dependencies) {

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
    val src: String = config.getString("paths.hadoop.v1.source")
    val target: String = config.getString("paths.hadoop.v1.target")
    if (new File(target).exists) Shell.rmDir(target)
    Shell.untar(src, target)
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
    val configPath: String = conf.getString("paths.hadoop.v1.conf")
    // core-site.xml
    var names: List[String] = conf.getStringList("hdfs.v1.core-site.names").asScala.toList
    var values: List[String] = conf.getStringList("hdfs.v1.core-site.values").asScala.toList
    var confString = configTemplate(names, values)
    scala.xml.XML.save(configPath + "core-site.xml", confString, "UTF-8", true, null)
    // hdfs-site.xml
    names = conf.getStringList("hdfs.v1.hdfs-site.names").asScala.toList
    values = conf.getStringList("hdfs.v1.hdfs-site.values").asScala.toList
    confString = configTemplate(names, values)
    scala.xml.XML.save(configPath + "hdfs-site.xml", confString, "UTF-8", true, null)
  }

  val configTemplate = { (names: List[String], values: List[String]) =>
    if(names.length != values.length)
      throw new RuntimeException("Name and Value Lists must have same number of elements!")

    <configuration>
      {(names, values).zipped.map{ (n, v) =>
      <property>
        <name>{n}</name>
        <value>{v}</value>
      </property>}}
    </configuration>
  }

  override def toString = "HDFS v1"

}
