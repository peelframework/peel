package org.peelframework.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

/** Spring Converter to convert Java Lists to Scala Lists
 *
 * @tparam T Type of list elements.
 */
class JavaCollectionToScalaSeq[T] extends Converter[java.util.Collection[T], scala.collection.Seq[T]] {

  def convert(l: java.util.Collection[T]): Seq[T] =  {
    l.asScala.toSeq
  }
}
