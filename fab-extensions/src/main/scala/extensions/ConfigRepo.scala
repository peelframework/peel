/**
 * Created by felix on 27.04.14.
 */
package extensions

object ConfigRepo {
  // create hdfs that is used as the injected dependency into hadoop and stratosphere
  val shell = BashShell
  lazy val hdfs = new HDFS(this)
  lazy val hadoop = new Hadoop(this)
  lazy val yarn = new YARN(this)
  lazy val stratosphere = new Stratosphere(this)
}
