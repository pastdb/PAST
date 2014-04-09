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
    val db = new Database(dbName, filesystem, config)
  }

  "A new Database" should "not exist" in new Builder {
    assert(db.exists === false)
  }

  it should "be creatable" in new Builder {
    db.create()
    assert(db.exists)
  }

  it should "be able to create timeseries" in new Builder {
    db.create()
    val schema = new Schema(("id", DBInt32), ("A", DBFloat32), ("B", DBInt64))
    assert(!db.hasTimeseries("T"))
    assert(db.createTimeseries("T", schema))
    assert(db.hasTimeseries("T"))
    assert(new Database(dbName, filesystem, config).hasTimeseries("T"))
    assert(new Database(dbName, filesystem, config).getTimeseries("T").get.schema == schema)
  }
}

