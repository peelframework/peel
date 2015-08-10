package org.peelframework.core

import java.net.URL

import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/** Spring [[org.springframework.context.ApplicationContext ApplicationContext]] factory. */
object PeelApplicationContext {

  /** Creates a [[org.springframework.context.ApplicationContext ApplicationContext]]
    *
    * @param experimentsXMLPath Optionally, a path to the `experiments.xml` file.
    * @return The constructed [[ApplicationContext]].
    */
  def apply(experimentsXMLPath: Option[String] = None): ApplicationContext = {
    // construct classpath
    val cp = Array(
      Option(getClass.getResource("/peel-core.xml")) /* */ map { x => s"classpath:${lastPartOf(x)}" },
      Option(getClass.getResource("/peel-extensions.xml")) map { x => s"classpath:${lastPartOf(x)}" },
      experimentsXMLPath /*                             */ map { x => s"file:$x" }
    ).flatten
    // construct and return application context
    val ac = new ClassPathXmlApplicationContext(cp, true)
    ac.registerShutdownHook()
    ac
  }

  /** Returns the last part of an URL, e.g. for `http://example.com/foo/bar.html` the result will be `bar.html`. */
  private def lastPartOf(url: URL): String = {
    url.getPath.split('/').reverse.head
  }
}
