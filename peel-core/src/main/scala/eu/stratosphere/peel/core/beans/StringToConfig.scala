package eu.stratosphere.peel.core.beans

import com.typesafe.config.{ConfigFactory, Config}
import org.springframework.core.convert.converter.Converter

class StringToConfig extends Converter[java.lang.String, Config] {

  def convert(s: java.lang.String): Config = {
    ConfigFactory.parseString(s)
  }
}
