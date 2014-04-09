import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest._
import past.storage.DBType._
import past.storage.{DBType, Schema, Timeseries}
import past.test.util.TestDirectory

class TimeseriesSpec extends FlatSpec with TestDirectory {

  val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())

  trait NameGenerator {
    val name = "pastdb_timeseries_%s".format(System.nanoTime)
  }

  trait Builder extends NameGenerator {
    // TODO: richer schema
    val schema = new Schema(("ts", DBType.DBInt32))
    val timeseries = new Timeseries(name, schema, testDirectory, filesystem)
  }

  "A non-existing Timeseries" should "not exist" in new NameGenerator {
    intercept[java.io.IOException] {
      new Timeseries(name, testDirectory, filesystem)
    }
  }

  it should "be creatable" in new Builder {
    assert(new Timeseries(name, testDirectory, filesystem).schema == schema)
  }

  "Data" should "be able to be inserted and retrieved" in new Builder  {
    val db = new Timeseries(name, testDirectory, filesystem)
    val data = List(1,2,3,4,5,6,7,8,9)
    db.insert(List(("ts",data)))
    db.get[Int](0 to 9,"ts").zip(data.toIterator).foreach{case (x,y) =>
      assert(x == y)
    }
  }
}
