package eu.stratosphere.peel.core.config

import com.typesafe.config.Config

trait Configurable {

  /** The Config instance associated with the object. */
  var config: Config

  /** Substitutes all config parameters `${id}` in `v` with their corresponding values defined in the enclosing `config`.
    *
    * @param v The string where the values should be substituted.
    * @return The subsituted version of v.
    * @throws com.typesafe.config.ConfigException.Missing if value is absent or null
    */
  def resolve(v: String) = substituteConfigParameters(v)(config)
}