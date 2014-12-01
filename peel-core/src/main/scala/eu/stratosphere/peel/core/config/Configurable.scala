package eu.stratosphere.peel.core.config

import com.typesafe.config.{Config, ConfigFactory}

trait Configurable {

  /** The Config instance associated with the object. */
  var config: Config

  /** Resovles `${foo.bar}` patterns in the given string using the current config.
   *
   * @param v The string to resolve.
   * @return A resolved version of the string.
   */
  def resolve(v: String) = ConfigFactory.parseString(s"_tmp_: $v").withFallback(config).resolve().getString("_tmp_")
}