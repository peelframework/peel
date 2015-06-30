package eu.stratosphere.peel.core

import java.io.File
import java.lang.{System => Sys}
import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.beans.system.System
import eu.stratosphere.peel.core.graph.{DependencyGraph, Node}
import eu.stratosphere.peel.core.util.shell
import org.slf4j.LoggerFactory

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

  final val logger = LoggerFactory.getLogger(this.getClass)

  def loadConfig(graph: DependencyGraph[Node], sys: System) = {
    logger.info(s"Loading configuration for system '${sys.beanName}'")

    // initial empty configuration
    val cb = new ConfigBuilder

    // load reference configuration
    cb.loadResource("reference.conf")

    // load systems configuration
    for (n <- graph.reverse.traverse(); if graph.descendants(sys).contains(n)) n match {
      case s: System =>
        // load reference.{system.defaultName}.conf
        cb.loadResource(s"reference.${s.beanName}.conf")
        // load {app.path.config}/{system.name}.conf
        cb.loadFile(s"${Sys.getProperty("app.path.config")}/${s.beanName}.conf")
        // load {app.path.config}/{app.hostname}/{system.name}.conf
        cb.loadFile(s"${Sys.getProperty("app.path.config")}/${Sys.getProperty("app.hostname")}/${s.beanName}.conf")
      case _ => Unit
    }

    // load {app.path.config}/application.conf
    cb.loadFile(s"${Sys.getProperty("app.path.config")}/application.conf")
    // load {app.path.config}/{app.hostname}/application.conf
    cb.loadFile(s"${Sys.getProperty("app.path.config")}/${Sys.getProperty("app.hostname")}/application.conf")

    // load current runtime config
    logger.info(s"Loading current runtime values as configuration")
    cb.append(currentRuntimeConfig())

    // load system properties
    logger.info(s"Loading system properties as configuration")
    cb.append(ConfigFactory.systemProperties)

    // resolve and return config
    logger.info(s"Resolving configuration")
    cb.resolve()
  }

  def loadConfig(graph: DependencyGraph[Node], exp: Experiment[System]) = {
    logger.info(s"Loading configuration for experiment '${exp.name}'")

    // initial empty configuration
    val cb = new ConfigBuilder

    // load reference configuration
    cb.loadResource("reference.conf")

    // load systems configuration
    for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) n match {
      case s: System =>
        // load reference.{system.defaultName}.conf
        cb.loadResource(s"reference.${s.beanName}.conf")
        // load {app.path.config}/{system.name}.conf
        cb.loadFile(s"${Sys.getProperty("app.path.config")}/${s.beanName}.conf")
        // load {app.path.config}/{app.hostname}/{system.name}.conf
        cb.loadFile(s"${Sys.getProperty("app.path.config")}/${Sys.getProperty("app.hostname")}/${s.beanName}.conf")
      case _ => Unit
    }

    // load {app.path.config}/application.conf
    cb.loadFile(s"${Sys.getProperty("app.path.config")}/application.conf")
    // load {app.path.config}/{app.hostname}/application.conf
    cb.loadFile(s"${Sys.getProperty("app.path.config")}/${Sys.getProperty("app.hostname")}/application.conf")

    // load the experiment config
    logger.info(s"Loading experiment configuration")
    cb.append(exp.config)

    // load current runtime config
    logger.info(s"Loading current runtime values as configuration")
    cb.append(currentRuntimeConfig())

    // load system properties
    logger.info(s"Loading system properties as configuration")
    cb.append(ConfigFactory.systemProperties)

    // resolve and return config
    logger.info(s"Resolving configuration")
    cb.resolve()
  }


  /** Loads default values from the current runtime config.
   *
   * @return current Config Object
   */
  private def currentRuntimeConfig() = {
    // initial empty configuration
    val runtimeConfig = new java.util.HashMap[String, Object]()
    // add current runtime values to configuration
    runtimeConfig.put("runtime.cpu.cores", Runtime.getRuntime.availableProcessors().asInstanceOf[Object])
    runtimeConfig.put("runtime.memory.max", Runtime.getRuntime.maxMemory().asInstanceOf[Object])
    runtimeConfig.put("runtime.hostname", (shell !! "echo $HOSTNAME").trim())
    runtimeConfig.put("runtime.disk.size", new File("/").getTotalSpace.asInstanceOf[Object])
    // return a config object
    ConfigFactory.parseMap(runtimeConfig)
  }

  private class ConfigBuilder {

    // initial empty configuration
    private var config = ConfigFactory.empty()

    // options for configuration parsing
    private val options = ConfigParseOptions.defaults().setClassLoader(this.getClass.getClassLoader)

    // helper function: append resource to current config
    def loadResource(name: String) = {
      if (Option(this.getClass.getResource(s"/$name")).isDefined) {
        logger.info(s"Loading resource $name")
        config = ConfigFactory.parseResources(name, options).withFallback(config)
      } else {
        logger.info(s"Skipping resource $name (does not exist)")
      }
    }
    // helper function: append file to current config
    def loadFile(path: String) = {
      if (Files.isReadable(Paths.get(path))) {
        logger.info(s"Loading file $path")
        config = ConfigFactory.parseFile(new File(path), options).withFallback(config)
      } else {
        logger.info(s"Skipping file $path (does not exist)")
      }
    }

    def append(other: Config) = config = other.withFallback(config)

    def resolve() = {
      val x = config.resolve()
      x
    }
  }
}
