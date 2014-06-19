package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{ExperimentRunner, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.ExecutionContext
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import java.io.File

class Hadoop(lifespan: Lifespan, dependencies: Set[System] = Set()) extends ExperimentRunner(lifespan, dependencies) {

  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
    configure(config)
  }

  override def run(ctx: ExecutionContext) = {
    logger.info("Running Hadoop Job...")
  }

  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
  }

  def update(): Unit = {
    logger.info("Updating " + toString + "...")
  }

  def configure(conf: Config) = {
    val configPath: String = conf.getString("paths.hadoop.v1.conf")
    // hadoop-env
    var names: List[String] = conf.getStringList("hadoop.v1.hadoop-env.names").asScala.toList
    var values: List[String] = conf.getStringList("hadoop.v1.hadoop-env.values").asScala.toList
    val envString = envTemplate(names, values)
    printToFile(new File(configPath + "hadoop-env.sh"))(p => {
      envString.foreach(p.println)
    })

    // mapred-site.xml
    names = conf.getStringList("hadoop.v1.mapred-site.names").asScala.toList
    values = conf.getStringList("hadoop.v1.mapred-site.values").asScala.toList
    val confString = configTemplate(names, values)
    scala.xml.XML.save(configPath + "mapred-site.xml", confString, "UTF-8", true, null)

  }

  val envTemplate = { (names: List[String], values: List[String]) =>
    if (names.length != values.length)
      throw new RuntimeException("Name and Value Lists must have same number of elements!")

    (names, values).zipped.map{ (n, v) => "export %s = %s \n".format(n, v)}

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

  override def toString: String = "Hadoop v1"
}
