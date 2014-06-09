package eu.stratosphere.fab.core.beans.system

import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import scala.collection.JavaConverters._


/**
 * Created by felix on 09.06.14.
 */
abstract class System(val name: String, val lifespan: Lifespan, val dependencies: java.util.Set[System]) {

  final val dependencySet: Set[System] = dependencies.asScala.toSet

  def setUp(): Unit

  def tearDown(): Unit

}
