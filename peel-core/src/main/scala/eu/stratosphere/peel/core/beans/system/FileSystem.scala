package eu.stratosphere.peel.core.beans.system

trait FileSystem {

  /**
   * Checks if a path exists.
   *
   * @param path The path to be checked.
   * @return True if the path exists in this file system.
   */
  def exists(path: String): Boolean

  /**
   * Remove a path and all its sub-paths from file system.
   *
   * @param path The path to remove.
   * @param skipTrash Skip the trash folder.
   * @return The exit code of the operation
   */
  def rmr(path: String, skipTrash: Boolean = true): Int

  /**
   * Copy the a path from local file system to this file.
   *
   * @param src The source path.
   * @param dst The destination path.
   * @return The exit code of the operation
   */
  def copyFromLocal(src: String, dst: String): Int
}
