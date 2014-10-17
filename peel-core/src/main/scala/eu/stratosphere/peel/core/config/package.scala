package eu.stratosphere.peel.core

import java.io.File
import java.lang.{System => Sys}
import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import eu.stratosphere.peel.core.beans.experiment.Experiment
import eu.stratosphere.peel.core.beans.system.System
import eu.stratosphere.peel.core.graph.{DependencyGraph, Node}
import org.slf4j.LoggerFactory

package object config {

  final val logger = LoggerFactory.getLogger(this.getClass)

  def loadConfig(graph: DependencyGraph[Node], sys: System) = {
    logger.info(s"Loading configuration for system '${sys.name}'")

    // initial empty configuration
    val cb = new ConfigBuilder

    // load reference configuration
    cb.loadResource("reference.conf")

    // load systems configuration
    for (n <- graph.reverse.traverse(); if graph.descendants(sys).contains(n)) n match {
      case s: System =>
        // load reference.{system.defaultName}.conf
        cb.loadResource(s"reference.${s.name}.conf")
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
        cb.loadResource(s"reference.${s.name}.conf")
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

    // load system properties
    logger.info(s"Loading system properties as configuration")
    cb.append(ConfigFactory.systemProperties)

    // resolve and return config
    logger.info(s"Resolving configuration")
    cb.resolve()
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
      }
    }
    // helper function: append file to current config
    def loadFile(path: String) = {
      if (Files.isReadable(Paths.get(path))) {
        logger.info(s"Loading file $path")
        config = ConfigFactory.parseFile(new File(path), options).withFallback(config)
      }
    }

    def append(other: Config) = config = other.withFallback(config)

    def resolve() = config.resolve()
  }
}
