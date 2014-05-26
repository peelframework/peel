package common

import com.sun.corba.se.impl.orbutil.graph.Graph

/**
 * Created by felix on 27.04.14.
 */
trait Controller extends App{
  def parseConfigFile(): List[LifecycleElement]
  def register(sc: SystemComponent): Unit
  def notifyComponents(le: LifecycleElement): Unit
}
