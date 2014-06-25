package eu.stratosphere.fab.extensions.beans.system.hadoop

import eu.stratosphere.fab.core.beans.system.{FileSystem, System}
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import com.typesafe.config.Config
import scala.collection.JavaConverters._
import java.io.{FileNotFoundException, File}
import eu.stratosphere.fab.core.beans.Shell


class HDFS(lifespan: Lifespan, dependencies: Set[System] = Set()) extends FileSystem(lifespan, dependencies) {

  val home: String = config.getString("paths.hadoop.v1.home")

  /**
   * setup hdfs
   *
   * Creates a complete hdfs installation with configuration
   * and starts a single node cluster
   */
  //TODO: allow for more nodes
  def setUp(): Unit = {
    logger.info("Setting up " + toString + "...")
    val src: String = config.getString("paths.hadoop.v1.source")
    val target: String = config.getString("paths.hadoop.v1.target")
    val user: String = config.getString("hdfs.v1.user.name")
    val group: String = config.getString("hdfs.v1.user.group")
    if (new File(home).exists) Shell.rmDir(home)
    Shell.untar(src, target)
    Shell.execute(("chown -R %s:%s " + home).format(user, group) , true)
    configure(config)
    Shell.execute(home + "bin/hadoop namenode -format -force", true)
    Shell.execute(home + "bin/start-dfs.sh", true)
    while(inSafemode) Thread.sleep(500)
  }

  /**
   * tear down hdfs
   *
   * Shuts down NameNode, Jobtracker and all other Nodes
   */
  //TODO remove data folders
  def tearDown(): Unit = {
    logger.info("Tearing down " + toString + "...")
    Shell.execute(home + "bin/stop-dfs.sh", true)
  }

  /**
   * updates hdfs
   * the system is shut down and set up again with different parameters
   *
   * TODO implement good way to update the parameters
   *  - give config object as parameter that can be used in setup and configure
   */
  def update(): Unit = {
    logger.info("Updating " + toString + "...")
    tearDown()
    setUp()
  }

  /**
   * checks if hdfs is in safemode
   * @return
   */
  def inSafemode: Boolean = {
    val msg: (String, String, Int) = Shell.execute(home + "bin/hadoop dfsadmin -safemode get", true)
    val status = msg._1.toLowerCase
    if (status.contains("off")) false else true
  }

  /**
   * copies the given file to hdfs input directory and returns the path to
   * the file in hdfs
   * @param from path on local file system
   * @return path on hdfs
   */
  def setInput(from: File): File = {
    val to: File = new File(config.getString("paths.hadoop.v1.input"), from.getName)
    logger.info("Copy Input data from %s to %s...".format(from, to))
    Shell.execute(home + "bin/hadoop fs -put %s %s".format(from, to), true)
    to
  }

  //TODO check if file already exists - if so, remove?
  /**
   * retrieve data from hdfs output folder
   * copied the data from the hdfs output folder to a folder on
   * the local file system
   * @param to path on local fs where the data is copied to from hdfs
   * @return path on local fs for the output file
   */
  def getOutput(from: File, to: File) = {
      logger.info("Copy Input data from %s to %s...".format(from, to))
      Shell.execute(home + "bin/hadoop fs -get %s %s".format(from, to), true)
  }

  /**
   * configure hdfs
   *
   * the necessary config files are created with the given configuration
   * if there are several runs, a new config file can be passed and new
   * configuration files in the hadoop conf folder are created (overwritten)
   * @param conf the configuration object that is used to read the parameters
   */
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

  override def toString = "HDFS v1"

}
