import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._
import past.storage.Database
import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Mixin that creates a temporary directory for the lifetime of a test.
 * The directory is stored in `testDirectory` as a `Path` instance.
 * TODO: Move this trait to a different place.
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
      //deleteFile(directory)
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

class DatabaseSpec extends FlatSpec with TestDirectory {

  val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())

  trait Builder {
    val config: Config = ConfigFactory.parseMap(Map("path" -> testDirectory.toString).asJava)
    val dbName = "pastdb_%s".format(System.nanoTime)
    val db = new Database(dbName, filesystem, config)
  }

  "A new Database" should "not exist" in new Builder {
    assert(db.exists === false)
  }

  it should "be creatable" in new Builder {
    db.create()
    assert(db.exists)
  }
}

