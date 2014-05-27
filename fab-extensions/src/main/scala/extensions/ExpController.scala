/**
 * Created by felix on 27.04.14.
 */
package extensions

import common._
import common.LifecycleElement
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import scala.xml.XML
import common.SystemComponent

class ExpController(env : { val hdfs: HDFS
                            val hadoop: Hadoop
                            val stratosphere: Stratosphere
                            val yarn: YARN},
                    val configPath: String) extends Controller {

  private val logger = LoggerFactory.getLogger(this.getClass)
  PropertyConfigurator.configure("/home/felix/IdeaProjects/fab-framework/shellLogger.properties")



  def parseConfigFile() = {
    val xml = XML.loadFile(configPath)

    // get experiments and systems
    val experiments = xml \ "experiment"
    val exSystems = xml \\ "system"

  }

  def register(sc: SystemComponent) {
    // register listener components
  }

  def notify(sys: => SystemComponent, e: ExpEvent) {
    sys.update(e)
  }

  def run() {
    notify(env.hdfs, new SetupEvent)
    notify(env.yarn, new SetupEvent)
    notify(env.yarn, new TearDownEvent)
    notify(env.hdfs, new TearDownEvent)
  }

}
