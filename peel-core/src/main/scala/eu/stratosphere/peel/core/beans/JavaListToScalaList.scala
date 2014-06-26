package eu.stratosphere.peel.core.beans

import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

class JavaListToScalaList[T] extends Converter[java.util.List[T], scala.collection.immutable.List[T]] {

  def convert(l: java.util.List[T]): List[T] =  {
    l.asScala.toList
  }
}
