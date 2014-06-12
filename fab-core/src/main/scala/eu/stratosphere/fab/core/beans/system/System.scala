package eu.stratosphere.fab.core.beans.system

import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan
import eu.stratosphere.fab.core.Node


/**
 * Created by felix on 09.06.14.
 */
abstract class System(val lifespan: Lifespan, val dependencies: Set[System]) extends Node{

  def setUp(): Unit

  def tearDown(): Unit

}
