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
package org.peelframework.core

import java.io.File
import java.lang.{System => Sys}
import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import org.peelframework.core.beans.experiment.Experiment
import org.peelframework.core.beans.system.System
import org.peelframework.core.graph.{DependencyGraph, Node}
import org.peelframework.core.util.console.ConsoleColorise
import org.slf4j.LoggerFactory

import scala.sys.process.Process

/** Package Object to handle the loading of configuration files
  *
  * Configuration files are loaded and resolved according to the
  * specifications of the [[https://github.com/typesafehub/config Typesafe Config docs]]
  * (check standard behavior section first).
  *
  * non-standard config files are read from {app.path.config} where host-specific configurations
  * are read from {app.path.config}/{app.hostname}
  *
  */
package object config {

  val parameter = """\$\{(\S+?)\}""".r // non greedy pattern for matching ${<id>} sequences

  final val logger = LoggerFactory.getLogger(this.getClass)

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getOptionalString(path: String): Option[String] = if (underlying.hasPath(path)) {
      Some(underlying.getString(path))
    } else {
      None
    }
  }

  def loadConfig() = {
    logger.info(s"Loading application configuration")

    // initial empty configuration
    val cb = new ConfigBuilder

    // load reference configuration
    cb.loadResource("reference.peel.conf")

    // load {app.path.config}/application.conf
    for (configPath <- Option(Sys.getProperty("app.path.config"))) {
      cb.loadFile(s"$configPath/application.conf")
    }
    // load {app.path.config}/{app.hostname}/application.conf
    for (configPath <- Option(Sys.getProperty("app.path.config")); hostname <- Option(Sys.getProperty("app.hostname"))) {
      cb.loadFile(s"$configPath/hosts/$hostname/application.conf")
    }

    // load current runtime config
    logger.info(s"+-- Loading current runtime values as configuration")
    cb.append(currentRuntimeConfig)

    // load system properties
    logger.info(s"+-- Loading system properties as configuration")
    cb.append(ConfigFactory.systemProperties)

    // resolve and return config
    logger.info(s"`-- Resolving configuration")
    cb.resolve()
  }

  def loadConfig(graph: DependencyGraph[Node], sys: System) = {
    logger.info(s"Loading configuration for system '${sys.beanName}'")

    // base path for bundle- and host- specific configurations
    val configPath = Sys.getProperty("app.path.config")
    // current hostname
    val hostname = Sys.getProperty("app.hostname")

    // get dependent systems
    val dependentSystems = for {
      System(s) <- graph.reverse.traverse()
      if graph.descendants(sys).contains(s)
    } yield s

    // initial empty configuration
    val cb = new ConfigBuilder

    // load reference configuration
    cb.loadResource("reference.peel.conf")

    // load `reference.{system.defaultName}.conf` for each dependent system
    for (s <- dependentSystems) {
      cb.loadResource(s"reference.${s.beanName}.conf")
    }
    // load `{app.path.config}/{system.name}.conf` for each dependent system
    for (s <- dependentSystems) {
      cb.loadFile(s"$configPath/${s.beanName}.conf")
    }
    // load `{app.path.config}/{app.hostname}/{system.name}.conf` for each dependent system
    for (s <- dependentSystems) {
      cb.loadFile(s"$configPath/hosts/$hostname/${s.beanName}.conf")
    }

    // load {app.path.config}/application.conf
    cb.loadFile(s"$configPath/application.conf")
    // load {app.path.config}/{app.hostname}/application.conf
    cb.loadFile(s"$configPath/hosts/$hostname/application.conf")

    // load current runtime config
    logger.info(s"+-- Loading current runtime values as configuration")
    cb.append(currentRuntimeConfig)

    // load system properties
    logger.info(s"+-- Loading system properties as configuration")
    cb.append(ConfigFactory.systemProperties)

    // resolve and return config
    logger.info(s"`-- Resolving configuration")
    cb.resolve()
  }

  def loadConfig(graph: DependencyGraph[Node], exp: Experiment[System]) = {

    // base path for bundle- and host- specific configurations
    val configPath = Sys.getProperty("app.path.config")
    // current hostname
    val hostname = Sys.getProperty("app.hostname")

    // get dependent systems
    val dependentSystems = for {
      System(s) <- graph.reverse.traverse()
      if graph.descendants(exp).contains(s)
    } yield s

    // initial empty configuration
    val cb = new ConfigBuilder

    // load reference configuration
    cb.loadResource("reference.peel.conf")

    // load `reference.{system.defaultName}.conf` for each dependent system
    for (s <- dependentSystems) {
      cb.loadResource(s"reference.${s.beanName}.conf")
    }
    // load `{app.path.config}/{system.name}.conf` for each dependent system
    for (s <- dependentSystems) {
      cb.loadFile(s"$configPath/${s.beanName}.conf")
    }
    // load `{app.path.config}/{app.hostname}/{system.name}.conf` for each dependent system
    for (s <- dependentSystems) {
      cb.loadFile(s"$configPath/hosts/$hostname/${s.beanName}.conf")
    }

    // load {app.path.config}/application.conf
    cb.loadFile(s"$configPath/application.conf")
    // load {app.path.config}/{app.hostname}/application.conf
    cb.loadFile(s"$configPath/hosts/$hostname/application.conf")

    // load the experiment config
    logger.info(s"+-- Loading experiment configuration")
    cb.append(exp.config)

    // load current runtime config
    logger.info(s"+-- Loading current runtime values as configuration")
    cb.append(currentRuntimeConfig)

    // load system properties
    logger.info(s"+-- Loading system properties as configuration")
    cb.append(ConfigFactory.systemProperties)

    // resolve and return config
    logger.info(s"`-- Resolving configuration")
    cb.resolve()
  }

  /** Substitutes all config parameters `\${id}` in `v` with their corresponding values defined in `config`.
    *
    * @param v The string where the values should be substituted.
    * @param config The config instance to use for parameter value lookup
    * @return The subsituted version of v.
    * @throws com.typesafe.config.ConfigException.Missing if value is absent or null
    */
  def substituteConfigParameters(v: String)(implicit config: Config) = {
    val keys = (for (m <- parameter findAllMatchIn v) yield m group 1).toSet.toList
    val vals = for (k <- keys) yield config.getAnyRef(k)
    (keys.map(k => s"$${$k}") zip vals.map(_.toString)).foldLeft(v) { case (z, (s, r)) => z replaceAllLiterally(s, r) }
  }

  /** Loads default values from the current runtime config.
    *
    * @return current Config Object
    */
  lazy val currentRuntimeConfig = {
    // initial empty configuration
    val runtimeConfig = new java.util.HashMap[String, Object]()
    // add current runtime values to configuration
    runtimeConfig.put("runtime.cpu.cores", Runtime.getRuntime.availableProcessors().asInstanceOf[Object])
    runtimeConfig.put("runtime.memory.max", Runtime.getRuntime.maxMemory().asInstanceOf[Object])
    runtimeConfig.put("runtime.hostname", hostname)
    runtimeConfig.put("runtime.disk.size", new File("/").getTotalSpace.asInstanceOf[Object])
    // return a config object
    ConfigFactory.parseMap(runtimeConfig)
  }

  lazy val hostname = {
    val name = Process("/bin/bash", Seq("-c", "CLASSPATH=;echo $HOSTNAME")).!!
    if (name.nonEmpty) name.trim else "localhost"
  }

  private class ConfigBuilder {

    // initial empty configuration
    private var config = ConfigFactory.empty()

    // options for configuration parsing
    private val options = ConfigParseOptions.defaults().setClassLoader(this.getClass.getClassLoader)

    // helper function: append resource to current config
    def loadResource(name: String) = {
      if (Option(this.getClass.getResource(s"/$name")).isDefined) {
        logger.info(s"+-- Loading resource $name")
        config = ConfigFactory.parseResources(name, options).withFallback(config)
      } else {
        logger.info(s"+-- Skipping resource $name (does not exist)".yellow)
      }
    }

    // helper function: append file to current config
    def loadFile(path: String) = {
      if (Files.isReadable(Paths.get(path))) {
        logger.info(s"+-- Loading file $path")
        config = ConfigFactory.parseFile(new File(path), options).withFallback(config)
      } else {
        logger.info(s"+-- Skipping file $path (does not exist)".yellow)
      }
    }

    def append(other: Config) = config = other.withFallback(config)

    def resolve() = config.resolve()
  }

}
