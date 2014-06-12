package eu.stratosphere.fab.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

/**
 * Created by felix on 12.06.14.
 */
class JavaSetToScalaSet[T] extends Converter[java.util.Set[T], scala.collection.immutable.Set[T]] {

  def convert(l: java.util.Set[T]): Set[T] =  {
    l.asScala.toSet
  }

}
