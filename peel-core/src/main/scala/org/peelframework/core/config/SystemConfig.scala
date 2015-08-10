package org.peelframework.core.config

import java.io._
import java.nio.file.{FileSystems, Files}

import com.samskivert.mustache.Mustache
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

/** A class representing a mapping from a HOCON config object to a set of system configuration files. */
case class SystemConfig(config: Config, entries: List[SystemConfig.Entry[Model]]) {

  /** Checks if the configuration has changed. */
  def hasChanged = (for (e <- entries) yield
  e.hasChanged(config)
    ).fold(false)(_ || _)

  /** Updates the configuration files based on the given config. Returns true if at least one file changed. */
  def update() = (for (e <- entries) yield
  e.update(config)
    ).fold(false)(_ || _)
}

/** Companion object for the [[org.peelframework.core.config.SystemConfig SystemConfig]] class. */
object SystemConfig {

  /** An entry represents a file with configuration parameters for the enclosing system.
    *
    * @param configKey The key under which the values for the configuration parameters for this file can be found.
    * @param filePath The path for the configuration file where these values are mapped.
    * @param templatePath The path to the template used to render the file.
    * @param mc The engine used to render the template.
    * @param m Implicit manifest for the model used when rendering the file associated with this entry.
    * @tparam T The model used when rendering the file associated with this entry.
    */
  case class Entry[+T <: Model](configKey: String, filePath: String, templatePath: String, mc: Mustache.Compiler)(implicit m: Manifest[T]) {

    private final val logger = LoggerFactory.getLogger(this.getClass)

    private final val INITIAL_BUFFER_SIZE = 4096

    private final val template = {
      val rs = Option(getClass.getResourceAsStream(templatePath))
      mc.compile(new BufferedReader(new InputStreamReader(
        rs.getOrElse(throw new RuntimeException("Cannot find template resoure %s".format(templatePath))))))
    }

    /** Check whether file represented by this entry has changed with resepect to a given `config` instance.
      *
      * @param config A reference config to use as 'ground truth' for the current set of config values.
      * @return True if the current file and the template rendered with the given `config` are binary incompatible.
      */
    def hasChanged(config: Config) = !java.util.Arrays.equals(fetch(), compute(config))

    /** Updates the file represented by this entry using the values provided by the given `config` instance. */
    def update(config: Config) = {
      val curr = fetch()
      val comp = compute(config)

      if (!java.util.Arrays.equals(curr, comp)) {
        logger.info(s"Updating configuration entry at %s".format(filePath))
        flush(comp)
        true
      } else {
        false
      }
    }

    private def fetch() = {
      val p = FileSystems.getDefault.getPath(filePath)
      if (Files.exists(p)) Files.readAllBytes(p) else Array[Byte]()
    }

    private def flush(b: Array[Byte]) = {
      val p = FileSystems.getDefault.getPath(filePath)
      Files.write(p, b)
    }

    private def compute(config: Config) = {
      val os = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE)
      val ow = new OutputStreamWriter(os)
      template.execute(Model.factory[T](config, configKey), ow)
      ow.flush()
      ow.close()
      os.toByteArray
    }
  }

}
