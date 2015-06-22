package eu.stratosphere.peel.core.beans.system

/** Lisfespan states of a system
  *
  * Different stages of a system's lifecycle.
  *
  * PROVIDED - the system is provided and is not going to be set up by peel
  * SUITE - The system is set up once for the whole suite and torn down at the end
  * EXPERIMENT - The system is set up and torn down for every single experiment
  *
  * Note: If a dependency of the system is changed (restarted with different values)
  * then all it's dependants are also reset, independent of the Lifespan. This is to
  * guarantee that all systems are consistent with the parameter settings.
 */
case object Lifespan extends Enumeration {
  type Lifespan = Value
  final val PROVIDED, SUITE, EXPERIMENT, JOB = Value
}
