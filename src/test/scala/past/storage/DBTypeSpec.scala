package past.storage

import past.storage.DBType._
import org.scalatest.FlatSpec
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.conf.Configuration
import past.test.util.TestDirectory

/**
 * Created by Eric on 03.04.14.
 */
class DBTypeSpec extends FlatSpec with TestDirectory{

  val numbers = (1 to 100).toList

  def serializeTest[T](values:List[T], typ:DBType[T]) = {
    val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())


    val out = filesystem.create(new Path(testDirectory, "test"))
    values.foreach(x => {
      typ.serialize(x, out)
    })
    out.close()

    val in = filesystem.open(new Path(testDirectory, "test"))
    values.foreach(x => {
      assert(x == typ.unserialize(in))
    })
    in.close()
  }

  "DBInt32" should "serialize/unserialize" in {
    assert(DBInt32.unserialize(DBInt32.serialize(11)) == 11)
    serializeTest(numbers, DBInt32)
  }

  "DBInt64" should "serialize/unserialize" in {
    assert(DBInt64.unserialize(DBInt64.serialize(549879)) == 549879)
    serializeTest(numbers.map(_.toLong), DBInt64)
  }

  "DBFloat32" should "serialize/unserialize" in {
    assert(DBFloat32.unserialize(DBFloat32.serialize(549.879f)) == 549.879f)
    serializeTest(numbers.map(_.toFloat), DBFloat32)
  }

  "DBFloat64" should "serialize/unserialize" in {
    assert(DBFloat64.unserialize(DBFloat64.serialize(5434679.879)) == 5434679.879)
    serializeTest(numbers.map(_.toDouble), DBFloat64)
  }

  "DBString(8)" should "serialize/unserialize" in {
    val typ = DBString(8)
    assert(typ.unserialize(typ.serialize("5434679.879")) == "5434679.")
    serializeTest(numbers.map(_.toString.padTo(8,'\0')), typ)
  }

}

