package org.peelframework.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

/** Spring Converter to convert Java Sets to Scala Sets
  *
  * @tparam T Type of set elements.
  */
class JavaSetToScalaSet[T] extends Converter[java.util.Set[T], scala.collection.immutable.Set[T]] {

  def convert(l: java.util.Set[T]): Set[T] =  {
    l.asScala.toSet
  }

}
