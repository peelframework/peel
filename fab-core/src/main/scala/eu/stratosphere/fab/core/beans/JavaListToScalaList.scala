package eu.stratosphere.fab.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

/**
 * Created by felix on 12.06.14.
 */
class JavaListToScalaList[T] extends Converter[java.util.List[T], scala.collection.immutable.List[T]] {

  def convert(l: java.util.List[T]): List[T] =  {
    l.asScala.toList
  }
}
