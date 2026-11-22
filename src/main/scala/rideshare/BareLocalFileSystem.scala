package rideshare

import java.io.{File, FileNotFoundException}

import org.apache.hadoop.fs.{FileStatus, LocalFileSystem, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission

/**
 * Local filesystem that never shells out for POSIX permissions.
 *
 * Hadoop's RawLocalFileSystem calls `winutils.exe` on Windows to read and set file
 * permissions. This class drops permission handling so Spark can read and write
 * local Parquet on Windows without downloading third-party native binaries.
 * It is only switched on for local runs on Windows (see [[Spark.session]]);
 * Linux, CI and clusters use the stock filesystem.
 */
class BareLocalFileSystem extends LocalFileSystem(new BareRawLocalFileSystem)

class BareRawLocalFileSystem extends RawLocalFileSystem {

  private def statusOf(path: Path, file: File): FileStatus =
    new FileStatus(
      if (file.isDirectory) 0L else file.length(),
      file.isDirectory,
      1,
      getDefaultBlockSize(path),
      file.lastModified(),
      makeQualified(path)
    )

  override def getFileStatus(f: Path): FileStatus = {
    val file = pathToFile(f)
    if (!file.exists()) throw new FileNotFoundException(s"File $f does not exist")
    statusOf(f, file)
  }

  override def getFileLinkStatus(f: Path): FileStatus = getFileStatus(f)

  override def listStatus(f: Path): Array[FileStatus] = {
    val file = pathToFile(f)
    if (!file.exists()) throw new FileNotFoundException(s"File $f does not exist")
    if (file.isFile) Array(statusOf(f, file))
    else Option(file.list()).getOrElse(Array.empty[String]).sorted.map { name =>
      val child = new Path(f, name)
      statusOf(child, new File(file, name))
    }
  }

  override def setPermission(p: Path, permission: FsPermission): Unit = ()

  override def setOwner(p: Path, username: String, groupname: String): Unit = ()
}
