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

import org.peelframework.core.util.shell
import java.io.File

import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.tools.nsc.{Global, Settings}

/** Spring application context factory. */
object PeelApplicationContext {

  /** Creates a Spring `ApplicationContext`.
    *
    * @param configPath Optionally, a path to the `experiments.xml` file.
    * @return The constructed `ApplicationContext`.
    */
  def apply(configPath: Option[String] = None): ApplicationContext = {

    val appCtx = (for (p <- configPath) yield {
      if (new File(s"$p/experiments.xml").isFile) {
        // XML-based configuration
        new ClassPathXmlApplicationContext(Array(s"file:$p/experiments.xml"), false)

      } else if (new File(s"$p/experiments.scala").isFile) {
        // Scala-based configuration
        compileScalaSources(new File(p))

        val cl = new java.net.URLClassLoader(
          Array(new File(".").toURI.toURL),  // Using current directory .
          this.getClass.getClassLoader)

        val ac = new AnnotationConfigApplicationContext()
        ac.setClassLoader(cl)
        ac.register(cl.loadClass("config.experiments"))
        ac

      } else {
        // neither `experiments.xml` nor `experiments.scala` is available
        // return an empty XML application context
        new ClassPathXmlApplicationContext()
      }
    }) getOrElse {
      // default: an empty XML application context
      new ClassPathXmlApplicationContext()
    }

    appCtx.registerShutdownHook()
    appCtx.refresh()

    appCtx
  }

  /** Compiles all `*.scala` sources in the given path. */
  private def compileScalaSources(root: File): Unit = {
    val set = new Settings()
    set.usejavacp.value = true
    val glb = new Global(set)
    val run = new glb.Run

    // compile all `*.scala` files that don't have a matching `*.class` file
    run.compile(shell.fileTree(root).map(_.toString).filter(f => {
      f.endsWith(".scala") && !new File(f.replace(".scala", ".class")).exists()
    }).toList)
  }
}
