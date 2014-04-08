import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest._
import past.storage.DBType._
import past.storage.Schema
import past.storage.Timeseries
import past.test.util.TestDirectory

class TimeseriesSpec extends FlatSpec with TestDirectory {

  val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())

  trait NameGenerator {
    val name = "pastdb_timeseries_%s".format(System.nanoTime)
  }

  trait Builder extends NameGenerator {
    // TODO: richer schema
    val schema = new Schema(("ts", DBInt32))
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
}
