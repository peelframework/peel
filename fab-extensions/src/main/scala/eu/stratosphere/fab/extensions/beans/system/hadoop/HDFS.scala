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
    val home: String = config.getString("paths.hadoop.v1.home")
    if (new File(home).exists) Shell.rmDir(home)
    Shell.untar(src, target)
    Shell.execute("chown -R felix:felix " + home , true)
    //Shell.execute("chmod 777 -R " + home + "/conf", true)
    configure(config)
    Shell.execute(home + "/bin/hadoop namenode -format", true)
    Shell.execute(home + "/bin/start-all.sh", true)
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
    val home: String = config.getString("paths.hadoop.v1.home")
    Shell.execute(home + "/bin/stop-all.sh", true)
  }

  def update(): Unit = {
    logger.info("Updating " + toString + "...")
    tearDown()
    setUp()
  }

  def configure(conf: Config) = {
    logger.info("Configuring " + toString + "...")
    val configPath: String = conf.getString("paths.hadoop.v1.home") + "/conf/"
    // hadoop-env
    var names: List[String] = conf.getStringList("hadoop.v1.hadoop-env.names").asScala.toList
    var values: List[String] = conf.getStringList("hadoop.v1.hadoop-env.values").asScala.toList
    val envString = envTemplate(names, values)
    printToFile(new File(configPath + "hadoop-env.sh"))(p => {
      envString.foreach(p.println)
    })
    // core-site.xml
    names = conf.getStringList("hdfs.v1.core-site.names").asScala.toList
    values = conf.getStringList("hdfs.v1.core-site.values").asScala.toList
    var confString = configTemplate(names, values)
    scala.xml.XML.save(configPath + "core-site.xml", confString, "UTF-8", true, null)
    // hdfs-site.xml
    names = conf.getStringList("hdfs.v1.hdfs-site.names").asScala.toList
    values = conf.getStringList("hdfs.v1.hdfs-site.values").asScala.toList
    confString = configTemplate(names, values)
    scala.xml.XML.save(configPath + "hdfs-site.xml", confString, "UTF-8", true, null)
    // mapred-site.xml
    names = conf.getStringList("hadoop.v1.mapred-site.names").asScala.toList
    values = conf.getStringList("hadoop.v1.mapred-site.values").asScala.toList
    confString = configTemplate(names, values)
    scala.xml.XML.save(configPath + "mapred-site.xml", confString, "UTF-8", true, null)
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

  val envTemplate = { (names: List[String], values: List[String]) =>
    if (names.length != values.length)
      throw new RuntimeException("Name and Value Lists must have same number of elements!")
    (names, values).zipped.map{ (n, v) => "export %s=\"%s\" \n".format(n, v)}
  }

  override def toString = "HDFS v1"

}
