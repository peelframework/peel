package eu.stratosphere.peel.core.beans.system

/** Exception thrown when a timeout is reached
 *
 * @param msg the message for the exception
 */
class SetUpTimeoutException(msg: String) extends RuntimeException(msg) {

}
