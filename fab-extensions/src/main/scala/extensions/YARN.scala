package extensions

import common._
import common.SCState._
import org.slf4j.LoggerFactory
import common.SCState.SCState
import common.LifecycleElement

/**
 * Created by felix on 20.05.14.
 *
 * Represents the Hadoop Yarn system
 */
class YARN(env: { val shell: Shell
                  val hdfs: HDFS}) extends SystemComponent{
  // describes the lifecycle of this system
  private val lifecycle: List[SCState]  = List(init, stop)
  private val logger = LoggerFactory.getLogger(this.getClass)

  val  getLifecycle: List[LifecycleElement] = for {lc <- lifecycle} yield LifecycleElement(lc, this.hashCode())

  def setup() = {
    env.shell.execute(env.hdfs.hadoopHome + "sbin/start-yarn.sh", true)
    startHistoryServer()
  }

  def startHistoryServer() = {
    env.shell.execute(env.hdfs.hadoopHome + "sbin/mr-jobhistory-daemon.sh start historyserver", true)
  }

  def stopHistoryServer() = {
    env.shell.execute(env.hdfs.hadoopHome + "sbin/mr-jobhistory-daemon.sh stop historyserver", true)
  }

  def tearDown() = {
    stopHistoryServer()
    env.shell.execute(env.hdfs.hadoopHome + "sbin/stop-yarn.sh", true)
  }

  def update(e: ExpEvent) = e match{
    case SetupEvent() => setup()
    case TearDownEvent() => tearDown()
    case _ => logger.info("Wrong Event was sent to YARN")
  }

}
