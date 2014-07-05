package eu.stratosphere.peel.core.beans.experiment

import java.io.File
import java.lang.{System => Sys}
import java.nio.file.{Files, Paths}

import com.typesafe.config._
import eu.stratosphere.peel.core.beans.system.{Lifespan, System}
import eu.stratosphere.peel.core.config.Configurable
import eu.stratosphere.peel.core.graph.{DependencyGraph, Node}
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanNameAware

class ExperimentSuite(final val experiments: List[Experiment[System]]) extends Node with BeanNameAware {

  final val logger = LoggerFactory.getLogger(this.getClass)

  var name = "experiments"

  def run() = {
    logger.info(s"Running experiments suite '$name'")
    logger.info(s"Constructing dependency graph for suite")
    val graph = createGraph()

    //TODO check for cycles in the graph
    if (graph.isEmpty) throw new RuntimeException("Suite is empty!")

    // resolve experiment configurations
    for (e <- experiments) e.config = loadConfig(graph, e)

    // generate runs to be executed
    val runs = for (e <- experiments; i <- 1 to e.runs; r <- Some(e.run(i)); if !r.isSuccessful) yield r
    // filter experiments in the relevant runs
    val exps = runs.foldRight(List[Experiment[System]]())((r, es) => if (es.isEmpty || es.head != r.exp) r.exp :: es else es)

    // SUITE lifespan
    try {
      logger.info("Executing experiments in suite")
      for (e <- exps) {
        // EXPERIMENT lifespan
        try {
          logger.info("#" * 60)
          logger.info("Current experiment is %s".format(e.name))

          // update config
          for (n <- graph.descendants(e).reverse) n match {
            case s: Configurable => s.config = e.config
            case _ => Unit
          }

          logger.info("Setting up systems with SUITE lifespan")
          for (n <- graph.reverse.traverse(); if graph.descendants(e).contains(n)) n match {
            case s: System => if ((Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan) && !s.isUp) s.setUp()
            case _ => Unit
          }

          logger.info("Updating systems with PROVIDED or SUITE lifespan")
          for (n <- graph.reverse.traverse(); if graph.descendants(e).contains(n)) n match {
            case s: System => if (Lifespan.PROVIDED :: Lifespan.SUITE :: Nil contains s.lifespan) s.update()
            case _ => Unit
          }

          logger.info("Setting up systems with EXPERIMENT lifespan")
          for (n <- graph.reverse.traverse(); if graph.descendants(e).contains(n)) n match {
            case s: System => if (Lifespan.EXPERIMENT :: Nil contains s.lifespan) s.setUp()
            case _ => Unit
          }

          logger.info("Materializing experiment input data sets")
          for (n <- e.inputs) n.materialize()

          for (r <- runs if r.exp == e) {
            for (n <- e.outputs) n.clean()
            r.execute() // run experiment
            for (n <- e.outputs) n.clean()
          }

        } catch {
          case e: Exception => logger.error(s"Exception of type ${e.getClass} in ExperimentSuite: ${e.getMessage}")
          case _: Throwable => logger.error(s"Exception in ExperimentSuite")

        } finally {
          logger.info("Tearing down systems with EXPERIMENT lifespan")
          for (n <- graph.traverse(); if graph.descendants(e).contains(n)) n match {
            case s: System => if (s.lifespan == Lifespan.EXPERIMENT) s.tearDown()
            case _ => Unit
          }
        }
      }

    }
    catch {
      case e: Exception => logger.error(s"Exception of type ${e.getClass} in ExperimentSuite: ${e.getMessage}")
      case _: Throwable => logger.error(s"Exception in ExperimentSuite")

    } finally {
      logger.info("#" * 60)
      logger.info("Tearing down systems with SUITE lifespan")
      for (n <- graph.traverse()) n match {
        case s: System => if (s.lifespan == Lifespan.SUITE) s.tearDown()
        case _ => Unit
      }
    }
  }

  private def loadConfig(graph: DependencyGraph[Node], exp: Experiment[System]) = {
    logger.info(s"Loading configuration for experiment ${exp.name}")
    // initial empty configuration
    var config = ConfigFactory.empty()

    // options for configuration parsing
    val options = ConfigParseOptions.defaults().setClassLoader(this.getClass.getClassLoader)
    // append resource to current config
    def loadResource(name: String) = {
      if (Option(this.getClass.getResource(s"/$name")).isDefined) {
        logger.info(s"Loading resource $name")
        config = ConfigFactory.parseResources(name, options).withFallback(config)
      }
    }
    def loadFile(path: String) = {
      if (Files.isReadable(Paths.get(path))) {
        logger.info(s"Loading file $path")
        config = ConfigFactory.parseFile(new File(path), options).withFallback(config)
      }
    }

    // load reference configuration
    loadResource("reference.conf")

    // load systems configuration
    for (n <- graph.reverse.traverse(); if graph.descendants(exp).contains(n)) n match {
      case s: System =>
        // load reference.{system.defaultName}.conf
        loadResource(s"reference.${s.name}.conf")
        // load {app.path.config}/{system.name}.conf
        loadFile(s"${Sys.getProperty("app.path.config")}/${s.beanName}.conf")
        // load {app.path.config}/{app.hostname}/{system.name}.conf
        loadFile(s"${Sys.getProperty("app.path.config")}/${Sys.getProperty("app.hostname")}/${s.beanName}.conf")
      case _ => Unit
    }

    // load {app.path.config}/application.conf
    loadFile(s"${Sys.getProperty("app.path.config")}/application.conf")
    // load {app.path.config}/{app.hostname}/application.conf
    loadFile(s"${Sys.getProperty("app.path.config")}/${Sys.getProperty("app.hostname")}/application.conf")

    // load the experiment config
    logger.info(s"Loading experiment configuration")
    config = exp.config.withFallback(config)

    // load system properties
    logger.info(s"Loading system properties as configation")
    config = ConfigFactory.systemProperties.withFallback(config)

    // resolve and return config
    logger.info(s"Resolving configuration")
    config.resolve()
  }

  /**
   * Create a directed Graph from all Experiments and their dependencies.
   *
   * @return Graph with systems as vertices and dependencies as edges
   */
  private def createGraph(): DependencyGraph[Node] = {

    val g = new DependencyGraph[Node]

    def processDependencies(s: System): Unit = {
      if (s.dependencies.nonEmpty) {
        // add dependencies to the system dependencies
        for (d <- s.dependencies) {
          g.addEdge(s, d)
          processDependencies(d)
        }
      }
    }

    for (e <- experiments) {
      g.addEdge(this, e)
      // add the experiment runner
      g.addEdge(e, e.runner)
      processDependencies(e.runner)

      // add the experiment inputs and their dependencies
      for (i <- e.inputs) {
        g.addEdge(e, i)
        // add the input dependencies
        for (x <- i.dependencies) {
          g.addEdge(i, x)
          processDependencies(x)
        }
      }
      // add the experiment outputs and their dependencies
      for (o <- e.outputs) {
        g.addEdge(e, o)
        // add the output dependency
        g.addEdge(o, o.fs)
        processDependencies(o.fs)
      }
    }

    g // return the graph
  }

  override def toString = "Experiment Suite"

  override def setBeanName(n: String): Unit = name = n
}
