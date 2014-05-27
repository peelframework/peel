/**
 * Created by felix on 27.04.14.
 */
package extensions
import common._
import common.SCState._
import common.SCState.SCState
import org.slf4j.LoggerFactory
import org.apache.log4j.PropertyConfigurator
import common.SCState.SCState
import common.SCState
import common.LifecycleElement

class HDFS(env : {val shell: Shell}) extends SystemComponent{

  // describes the lifecycle of this system
  private val lifecycle: List[SCState]  = List(init, stop)
  private val logger = LoggerFactory.getLogger(this.getClass)
  PropertyConfigurator.configure("/home/felix/IdeaProjects/fab-framework/shellLogger.properties")


  private val deployPath = "/home/felix/IdeaProjects/fab-framework/archives/"
  private val deployTarget = "/home/felix/IdeaProjects/fab-framework/systems/"
  val hadoopHome = "/home/felix/IdeaProjects/fab-framework/systems/hadoop-2.2.0/"
  private val templateDir = "/home/felix/IdeaProjects/fab-framework/templates/*"
  private val hadoopConfDir = "/home/felix/IdeaProjects/fab-framework/systems/hadoop-2.2.0/etc/hadoop/"
  private val hadoopTmpDir = "/home/felix/app/hadoop/tmp"

  val  getLifecycle: List[LifecycleElement] = for {lc <- lifecycle} yield LifecycleElement(lc, this.hashCode())

  def setup() {
    logger.info("setting up hdfs")
    //remove directories
    env.shell.execute("rm -r " + hadoopTmpDir, true)
    env.shell.execute("rm -r " + hadoopHome, true)
    // untar
    env.shell.execute("tar -xzvf " + deployPath + "hadoop-2.2.0.tar.gz " + " -C " + deployTarget, true)
    // change permissions
    env.shell.execute("chown -R felix:hadoop " + hadoopHome, true)
    // export hadoop home
    env.shell.execute("export HADOOP_HOME=" + hadoopHome, true)
    // make hadoop tmp directory
    env.shell.execute("mkdir -p " + hadoopTmpDir, true)
    // change permissions
    env.shell.execute("chown -R felix:hadoop " + hadoopTmpDir, true)
    // copy configuration files
    env.shell.execute("rm " + hadoopConfDir + "core-site.xml", true)
    env.shell.execute("cp " + templateDir + " " + hadoopConfDir, true)
    // format hdfs filesystem via namenode
    env.shell.execute(hadoopHome + "bin/hdfs namenode -format", true)
    //start hdfs
    logger.info("Starting hdfs")
    env.shell.execute(hadoopHome + "sbin/start-dfs.sh", true)
    //print status
    //env.shell.execute(hadoopHome + "bin/hdfs fsadmin -report", true)
    //start datanode
  }

  /**
   * Start a datanode on localhost
   *
   * TODO
   * cluster-setup
   */
  def startDataNode() = {
    env.shell.execute(hadoopHome + "bin/hdfs datanode", true)
  }

  def shutDownDataNode() = {
    env.shell.execute(hadoopHome + "/bin/hdfs datanode -stop", true)
  }

  def shutDownNameNode() = {
    env.shell.execute(hadoopHome + "/bin/hdfs namenode -stop", true)
  }

  def mkDir(name: String) = {
    env.shell.execute(hadoopHome + "bin/hdfs dfs -mkdir -p " + name, true)
  }

  /**
   * Copy data to hdfs
   * @param src path to the file to copy
   */
  def copyData(src: List[String], target: String) = {
    val sourcePath = src.mkString(" ")
    logger.info("copying data from %s to %s" format (sourcePath, target))
    env.shell.execute(hadoopHome + "bin/hdfs dfs -put " + sourcePath + " hdfs://" + target, true)
  }

  def tearDown() {
    logger.info("tearing down hdfs...")
    env.shell.execute(hadoopHome +  "sbin/stop-dfs.sh", true)
  }

  def update(e: ExpEvent) = e match {
    case SetupEvent() => setup()
    case TearDownEvent() => tearDown()
    case DataEvent(src, trgt) => copyData(src, trgt)
    case _ => logger.info("Wrong event was sent to HDFS")
  }


  override def toString(): String = {
    "HDFS System Component"
  }
}
