/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.core.config

import java.util

import com.typesafe.config.{ConfigRenderOptions, Config, ConfigObject}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** A model for the different configuration file types specified by systems (e.g. .yaml, .env, hosts). */
trait Model {

  case class Pair(name: String, value: Any) {}
  case class Section(name: String, entries: util.List[Pair]) {}

}

/** Companion object for the [[org.peelframework.core.config.Model Model]] trait. */
object Model {

  /** A model for HOCON templates.
    *
    * Consists of a single entry `value` which is constructed by rendering the given `c` HOCON Config object.
    *
    * @param c The HOCON config to use when constructing the model.
    * @param prefix The prefix path which has to be rendered.
    */
  class HOCON(val c: Config, val prefix: String) extends Model {

    val value = c.getValue(prefix).render(ConfigRenderOptions.defaults().setOriginComments(false))
  }

  /** A model for (key, value) based *-site.xml files (e.g. etc/hadoop/hdfs-site.xml).
    *
    * Consists of a single entry `properties` which is constructed by collecting all (key, value) pairs under
    * the specified `prefix` path.
    *
    * @param c The HOCON config to use when constructing the model.
    * @param prefix The prefix path which has to be rendered.
    */
  class Site(val c: Config, val prefix: String) extends Model {

    val properties = {

      val buffer = ListBuffer[Pair]()

      def sanitize(s: String) = s
        .stripPrefix(s"$prefix.") // remove prefix
        .stripSuffix(".\"_root_\"") // remove ._root_ suffix
        .replaceAll("\\.\"(\\d+)\"", ".$1") // unquote numeric path components, e.g. foo."1" => foo.1

      def collect(c: Config): Unit = {
        for (e <- c.entrySet().asScala) e.getValue match {
          case c: Config => collect(c)
          case _ => buffer += Pair(sanitize(e.getKey), c.getString(e.getKey))
        }
      }

      collect(c.withOnlyPath(prefix))

      buffer.toList.sortBy(x => x.name).asJava
    }
  }

  class INI(val c: Config, val prefix: String) extends Model {

    val sections = {
      val sectionBuffer = ListBuffer[Section]()

      def sanitize(s: String) =
        s.stripPrefix(s"$prefix.") // remove prefix

      def fixRoot(s: String) = if (s == "_root_") null else s

      def collectPairs(c: Config, name: String): Unit = {
        val buffer = ListBuffer[Pair]()

        for (e <- c.entrySet().asScala) {
          val key = sanitize(e.getKey)
            .replace("\"_root_\"", "_root_")
            .stripPrefix(s"$name.")
          buffer += Pair(key, c.getString(e.getKey))
        }

        sectionBuffer += Section(fixRoot(name), buffer.toList.asJava)
      }

      for (e <- c.getObject(prefix).entrySet().asScala) {
        val name = sanitize(e.getKey)
        collectPairs(c.withOnlyPath(s"$prefix.$name"), name)
      }

      sectionBuffer.toList.asJava
    }

  }

  /** A model for environment files (e.g., etc/hadoop/hadoop-env.sh).
    *
    * The children of the specified `prefix` path in the given `c` config are converted as (key, value) pairs in a
    * hash table.
    *
    * @param c The HOCON config to use when constructing the model.
    * @param prefix The prefix path which has to be rendered.
    */
  class Env(val c: Config, val prefix: String) extends java.util.HashMap[String, String] with Model {

    // constructor
    {
      def collect(c: Config): Unit = {
        for (e <- c.entrySet().asScala) e.getValue match {
          case c: Config => collect(c)
          case _ => this.put(e.getKey.stripPrefix(s"$prefix.").stripSuffix(".\"_root_\""), c.getString(e.getKey))
        }
      }

      collect(c.withOnlyPath(prefix))
    }
  }

  /** A model for (key, value) based files (e.g. conf/flink-conf.yaml or etc/hadoop/capacity-manager.xml).
    *
    * Consists of multiple (key, value) entries which are constructed by collecting all values under
    * the specified `prefix` path. Intermediate paths are thereby flattened, i.e.
    *
    * {{{
    *   yaml {
    *     foo {
    *       bar = 42
    *     }
    *   }
    * }}}
    *
    * will amount to a `('foo.bar', 42)` pair. The model extends `java.util.HashMap` and so the values can be
    * accessed directly by key in the template engine (e.g., {{foo.bar}}).
    *
    * @param c The HOCON config to use when constructing the model.
    * @param prefix The prefix path which has to be rendered.
    */
  class KeyValue(val c: Config, val prefix: String) extends util.HashMap[String, Object] with Model {

    // constructor
    {
      def collect(c: ConfigObject, m: util.HashMap[String, Object]): Unit = {
        val keys = (for (x <- c.entrySet().asScala) yield x.getKey.split('.').head).toSet
        for (k <- keys) c.get(k) match {
          case v: ConfigObject =>
            val child = new util.HashMap[String, Object]
            m.put(k, child)
            collect(v, child)
          case _ =>
            m.put(k, c.get(k).unwrapped())
        }
      }

      collect(c.getConfig(prefix).root(), this)
      Unit
    }
  }

  /** Alias for Model.KeyValue.
   *
   */
  class Yaml(override val c: Config, override val prefix: String) extends KeyValue(c, prefix) { }

  /** A model for list based hosts files (e.g. etc/hadoop/slaves).
    *
    * Consists of a single entry `hosts` which is constructed by extracting the string list value under the
    * specified `key` in the given `c` config.
    *
    * @param c The HOCON config to use when constructing the model.
    * @param key The key under which the hosts list can be found in the config.
    */
  class Hosts(val c: Config, val key: String) extends Model {

    val hosts = c.getStringList(key)
  }

  /** Factory method.
    *
    * @param config The config to use for model data extraction.
    * @param prefix The config prefix under which the data will be looked up.
    * @param m The implicit manifest of the model type.
    * @tparam T The model type to construct.
    * @return A model of type T to be used for template rendering.
    */
  def factory[T <: Model](config: Config, prefix: String)(implicit m: Manifest[T]) = {
    val constructor = m.runtimeClass.getConstructor(classOf[Config], classOf[String])
    constructor.newInstance(config, prefix)
  }
}
