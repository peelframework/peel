package eu.stratosphere.fab.core.beans.system

import java.io.File
import eu.stratosphere.fab.core.beans.system.Lifespan.Lifespan

/**
 * Created by felix on 23.06.14.
 */
abstract class FileSystem(lifespan: Lifespan, dependencies: Set[System]) extends System(lifespan, dependencies) {
  def setInput(from: File): File

  def getOutput(from: File, to: File)
}
