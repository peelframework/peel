/**
 * Created by felix on 27.04.14.
 */
package extensions

import common._
import common.SCState.SCState
import common.LifecycleElement
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import scala.xml.XML

class ExpController(env : { val hdfs: HDFS
                            val hadoop: Hadoop
                            val stratosphere: Stratosphere
                            val yarn: YARN},
                    val configPath: String) extends Controller {

  private val logger = LoggerFactory.getLogger(this.getClass)
  PropertyConfigurator.configure("/home/felix/IdeaProjects/fab-framework/shellLogger.properties")

  private def getListeners(): List[SystemComponent] = List(env.hdfs)

  def parseConfigFile(): List[LifecycleElement] = {
    val xml = XML.loadFile(configPath)

    // get experiments and systems
    val experiments = xml \ "experiment"
    val systems = xml \\ "system"

    // create graph from each experiment --> build union of graphs / optimized version

    // for testing, return list of lifecycle elements
    val listeners = getListeners()
    val lifecycle: List[LifecycleElement]  = {for {s <- listeners} yield s.getLifecycle}.reverse.flatMap(x => x)
    // val setupEvents = lifecycle filter (x => x._1 == SCState.init)
    println(lifecycle)
    lifecycle
  }

  def register(sc: SystemComponent) {
    // register listener components
  }

  def notifyComponents(le: LifecycleElement) {
    // traverse graph and notify systems/tasks
    println("notifying all components")
    for {sc <- getListeners()} yield sc.update(le)
  }

  def run() {
    val states = parseConfigFile()
    for {s <- states} yield notifyComponents(s)
  }

}
