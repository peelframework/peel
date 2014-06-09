package eu.stratosphere.fab.core.beans.system

/**
 * Created by felix on 09.06.14.
 */
case object Lifespan extends Enumeration {
  type Lifespan = Value
  final val SUITE, EXPERIMENT_SEQUENCE, EXPERIMENT, EXPERIMENT_RUN = Value
}
