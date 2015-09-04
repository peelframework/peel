/**
 * Copyright (C) 2014 TU Berlin (peel@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.peelframework.core

import java.net.URL

import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/** Spring application context factory. */
object PeelApplicationContext {

  /** Creates a Spring `ApplicationContext`.
    *
    * @param experimentsXMLPath Optionally, a path to the `experiments.xml` file.
    * @return The constructed `ApplicationContext`.
    */
  def apply(experimentsXMLPath: Option[String] = None): ApplicationContext = {
    // construct classpath
    val cp = Array(
      experimentsXMLPath map { x => s"file:$x" }
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
