package eu.stratosphere.peel.core.config

import java.io._
import java.nio.file.{FileSystems, Files}

import com.samskivert.mustache.Mustache
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
// TODO documentation
case class SystemConfig(config: Config, entries: List[SystemConfig.Entry[Model]]) {

  def hasChanged = (for (e <- entries) yield
    e.hasChanged(config)
    ).fold(false)(_ || _)

  def update() = (for (e <- entries) yield
    e.update(config)
    ).fold(false)(_ || _)
}

object SystemConfig {

  case class Entry[+T <: Model](configKey: String, filePath: String, templatePath: String, mc: Mustache.Compiler)(implicit m: Manifest[T]) {

    private final val logger = LoggerFactory.getLogger(this.getClass)

    private final val INITIAL_BUFFER_SIZE = 4096

    private final val template = {
      val rs = Option(getClass.getResourceAsStream(templatePath))
      mc.compile(new BufferedReader(new InputStreamReader(
        rs.getOrElse(throw new RuntimeException("Cannot find template resoure %s".format(templatePath))))))
    }

    def hasChanged(config: Config) = !java.util.Arrays.equals(fetch(), compute(config))

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
