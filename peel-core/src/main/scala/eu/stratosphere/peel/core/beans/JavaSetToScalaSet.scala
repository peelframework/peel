package eu.stratosphere.peel.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

class JavaSetToScalaSet[T] extends Converter[java.util.Set[T], scala.collection.immutable.Set[T]] {

  def convert(l: java.util.Set[T]): Set[T] =  {
    l.asScala.toSet
  }

}
