package past.test.util

import org.scalatest._
import org.apache.hadoop.fs.Path
import java.io.File
import scala.util.Random

/**
 * Mixin that creates a temporary directory for the lifetime of a test.
 * The directory is stored in `testDirectory` as a `Path` instance.
 * TODO: Add this somehow in the docs.
 */
trait TestDirectory extends SuiteMixin { this: Suite =>

  var testDirectory: Path = _

  abstract override def withFixture(test: NoArgTest) = {
    var directory: File = null

    do {
      directory = new File(System.getProperty("java.io.tmpdir"),
        "test_%s_%s".format(System.nanoTime, Random.alphanumeric.take(9).toList.mkString))
    } while (!directory.mkdir())

    testDirectory = new Path(directory.getPath())
    try super.withFixture(test)
    finally {
      deleteFile(directory)
    }
  }

  private def deleteFile(file: File) {
    if (!file.exists) return
    if (file.isFile) {
      file.delete()
    }
    else {
      file.listFiles().foreach(deleteFile)
      file.delete()
    }
  }
}

