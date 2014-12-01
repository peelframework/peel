package eu.stratosphere.peel.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

/** Spring Converter to convert Java Lists to Scala Lists
 *
 * @tparam T Type of list elements.
 */
class JavaListToScalaList[T] extends Converter[java.util.List[T], scala.collection.immutable.List[T]] {

  def convert(l: java.util.List[T]): List[T] =  {
    l.asScala.toList
  }
}
