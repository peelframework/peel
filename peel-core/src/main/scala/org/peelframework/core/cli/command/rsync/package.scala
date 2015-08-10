package org.peelframework.core.cli.command

import java.nio.file.Path

package object rsync {

  case class FolderEntry(
    src: Path,
    dst: Path,
    inc: Seq[String] = Seq.empty[String],
    exc: Seq[String] = Seq.empty[String]) {}
}
