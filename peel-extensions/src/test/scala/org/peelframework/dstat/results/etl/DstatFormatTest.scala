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
package org.peelframework.dstat.results.etl

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DstatFormatTest extends FlatSpec with Matchers {

  import DstatEventExtractor.Format._

  "Title" should "match" in {
    """ "Dstat 0.7.2 CSV output" """.trim match {
      case Title(version) =>
        version shouldBe "0.7.2"
    }
  }

  "AuthorInfo" should "match" in {
    """ "Author:","Foo Bar <foobar@example.com>",,,,"URL:","http://foo.bar/dstat/" """.trim match {
      case AuthorInfo(aut, url) =>
        aut shouldBe "Foo Bar <foobar@example.com>"
        url shouldBe "http://foo.bar/dstat/"
    }
  }

  "MachineInfo" should "match" in {
    """ "Host:","foo-11",,,,"User:","alexander" """.trim match {
      case MachineInfo(host, user) =>
        host shouldBe "foo-11"
        user shouldBe "alexander"
    }
  }

  "CommandInfo" should "match" in {
    """ "Cmdline:","dstat --epoch --cpu -C total,0,1 --output /f/b.csv 1",,,,"Date:","31 May 2016 13:07:57 CEST" """.trim match {
      case CommandInfo(cmnd, date) =>
        cmnd shouldBe "dstat --epoch --cpu -C total,0,1 --output /f/b.csv 1"
        date shouldBe "31 May 2016 13:07:57 CEST"
    }
  }
}
