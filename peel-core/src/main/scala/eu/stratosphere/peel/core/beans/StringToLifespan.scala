package eu.stratosphere.peel.core.beans

import eu.stratosphere.peel.core.beans.system.Lifespan
import org.springframework.core.convert.converter.Converter

/** Spring Converter to convert Java Strings to [[eu.stratosphere.peel.core.beans.system.Lifespan Lifespan]] Values
  *
  */
class StringToLifespan extends Converter[String, Lifespan.Value]{

  def convert(s: String): Lifespan.Value = {
    if (s == "PROVIDED") {
      Lifespan.PROVIDED
    }
    else if (s == "SUITE") {
      Lifespan.SUITE
    }
    else if (s == "EXPERIMENT") {
      Lifespan.EXPERIMENT
    }
    else if (s == "JOB") {
      Lifespan.JOB
    }
    else {
      throw new IllegalArgumentException(s + " can not be converted to Scala Lifecycle Value!")
    }
  }
}
