package org.peelframework.core.beans

import com.typesafe.config.{ConfigFactory, Config}
import org.springframework.core.convert.converter.Converter

/** Spring Converter to convert Java Strings to Config Objects using the `com.typesafe.config.ConfigFactory`. */
class StringToConfig extends Converter[java.lang.String, Config] {

  def convert(s: java.lang.String): Config = {
    ConfigFactory.parseString(s)
  }
}
