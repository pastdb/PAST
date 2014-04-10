import com.typesafe.config.{Config, ConfigFactory}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest._
import past.storage.DBType._
import past.storage.Database
import past.storage.Schema
import past.test.util.TestDirectory
import scala.collection.JavaConverters._

class DatabaseSpec extends FlatSpec with TestDirectory {

  val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())

  trait Builder {
    val config: Config = ConfigFactory.parseMap(Map("path" -> testDirectory.toString).asJava)
    val dbName = "pastdb_%s".format(System.nanoTime)
  }

  "A non-created Database" should "not exist" in new Builder {
    assert(!Database.exists(dbName, testDirectory, filesystem))
  }

  it should "be creatable" in new Builder {
    val db = new Database(dbName, filesystem, config)
    assert(Database.exists(dbName, testDirectory, filesystem))
  }

  "A created Database" should "not have a non-created timeseries" in new Builder {
    val db = new Database(dbName, filesystem, config)
    assert(!db.hasTimeseries("T"))
  }

  it should "be able to create timeseries" in new Builder {
    val db = new Database(dbName, filesystem, config)
    val schema = new Schema(("id", DBInt32), ("A", DBFloat32), ("B", DBInt64))
    assert(db.createTimeseries("T", schema))
    assert(db.hasTimeseries("T"))
    assert(new Database(dbName, filesystem, config).hasTimeseries("T"))
  }

  it should "be able to load existing timeseries" in new Builder {
    val db = new Database(dbName, filesystem, config)
    val schema = new Schema(("id", DBInt32), ("A", DBFloat32), ("B", DBInt64))
    db.createTimeseries("T", schema)
    assert(db.hasTimeseries("T"))
    assert(schema ==
      new Database(dbName, filesystem, config).getTimeseries("T").get.schema)
  }
}

