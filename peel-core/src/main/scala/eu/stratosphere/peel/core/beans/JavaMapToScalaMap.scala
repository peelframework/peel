package eu.stratosphere.peel.core.beans

import org.springframework.core.convert.converter.Converter

import scala.collection.JavaConverters._

/** Spring Converter to convert Java Maps to Scala Maps.
  *
  * @tparam K Key type.
  * @tparam V Value type.
  */
class JavaMapToScalaMap[K, V] extends Converter[java.util.Map[K, V], scala.collection.mutable.Map[K, V]] {

  def convert(m: java.util.Map[K, V]): scala.collection.mutable.Map[K, V] =  {
    m.asScala
  }

}
