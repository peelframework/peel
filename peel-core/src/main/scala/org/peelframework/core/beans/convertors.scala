package org.peelframework.core.beans

import com.typesafe.config.{ConfigFactory, Config}
import org.peelframework.core.beans.system.Lifespan
import org.springframework.core.convert.converter.Converter
import scala.collection.JavaConverters._

/** A package with Spring Convertors. */
object convertors {

  /* --------------------------------------------------------------------------
   * converters for Scala collections
   * ----------------------------------------------------------------------- */

  /** Spring Converter to convert Java Lists to Scala Lists.
    *
    * @tparam T Type of list elements.
    */
  class JavaCollectionToScalaSeq[T] extends Converter[java.util.Collection[T], Seq[T]] {

    def convert(l: java.util.Collection[T]): Seq[T] = {
      l.asScala.toSeq
    }
  }

  /** Spring Converter to convert Java Sets to Scala Sets.
    *
    * @tparam T Type of set elements.
    */
  class JavaSetToScalaSet[T] extends Converter[java.util.Set[T], Set[T]] {

    def convert(l: java.util.Set[T]): Set[T] = {
      l.asScala.toSet
    }
  }

  /** Spring Converter to convert Java Maps to Scala Maps.
    *
    * @tparam K Key type.
    * @tparam V Value type.
    */
  class JavaMapToScalaMap[K, V] extends Converter[java.util.Map[K, V], scala.collection.mutable.Map[K, V]] {

    def convert(m: java.util.Map[K, V]): scala.collection.mutable.Map[K, V] = {
      m.asScala
    }
  }

  /** Spring Converter to convert an object ot a singleton Scala Seq.
    *
    * @tparam T Type of set elements.
    */
  class ObjectToScalaSeq[T] extends Converter[T, Seq[T]] {

    def convert(l: T): Seq[T] = {
      Seq(l)
    }
  }

  /* --------------------------------------------------------------------------
   * string converters for custom classes
   * ----------------------------------------------------------------------- */

  /** Spring Converter to convert Java Strings to Config Objects using the `com.typesafe.config.ConfigFactory`. */
  class StringToConfig extends Converter[java.lang.String, Config] {

    def convert(s: java.lang.String): Config = {
      ConfigFactory.parseString(s)
    }
  }

  /** Spring Converter to convert Java Strings to [[Lifespan]] Values. */
  class StringToLifespan extends Converter[String, Lifespan.Value] {

    def convert(s: String): Lifespan.Value = s match {
      case "PROVIDED" =>
        Lifespan.PROVIDED
      case "SUITE" =>
        Lifespan.SUITE
      case "EXPERIMENT" =>
        Lifespan.EXPERIMENT
      case "JOB" =>
        Lifespan.JOB
      case _ =>
        throw new IllegalArgumentException(s + " can not be converted to Scala Lifecycle Value!")
    }
  }

}
