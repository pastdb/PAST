import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._
import past.storage.DBType._
import past.storage.Schema
import past.test.util.TestDirectory

class SchemaSpec extends FlatSpec with TestDirectory {
  "A Schema" should "not allow duplicate fields" in {
    intercept[java.lang.IllegalArgumentException] {
      new Schema(("TS", DBInt32), ("A", DBInt32), ("A", DBFloat64))
    }
    intercept[java.lang.IllegalArgumentException] {
      new Schema(("A", DBInt32), ("A", DBInt32))
    }
  }

  it should "support save and load" in {
    val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())
    val schema = new Schema(("TS", DBInt32), ("A", DBInt32), ("B", DBFloat64))
    val path = new Path(testDirectory, "schema_%s".format(System.nanoTime))
    schema.save(path, filesystem)
    assert(Schema.load(path, filesystem).fields == schema.fields)
  }

  it should "support equality tests" in {
    assert(new Schema(("ts", DBInt32)) == new Schema(("ts", DBInt32)))
    assert(new Schema(("ts", DBInt32)) != new Schema(("ts", DBFloat32)))
    assert(new Schema(("Ts", DBInt32)) != new Schema(("id", DBInt32)))
  }
}

