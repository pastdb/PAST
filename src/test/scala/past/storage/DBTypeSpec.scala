package past.storage

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import past.storage.DBType._
import org.scalatest.FlatSpec

/**
 * Created by Eric on 03.04.14.
 */
class DBTypeSpec extends FlatSpec{

  val numbers = (1 to 100).toList

  def serializeTest[T](values:List[T], typ:DBType[T]) = {
    val out = new ByteArrayOutputStream()
    values.foreach(x => {
      out.reset()
      typ.serialize(x, out)
      assert(x == typ.unserialize(new ByteArrayInputStream(out.toByteArray)))
    })
  }

  "DBInt32" should "serialize/unserialize" in {
    serializeTest(numbers, DBInt32)
  }

  "DBInt64" should "serialize/unserialize" in {
    serializeTest(numbers.map(_.toLong), DBInt64)
  }

  "DBFloat32" should "serialize/unserialize" in {
    serializeTest(numbers.map(_.toFloat), DBFloat32)
  }

  "DBFloat64" should "serialize/unserialize" in {
    serializeTest(numbers.map(_.toDouble), DBFloat64)
  }

  "DBString(8)" should "serialize/unserialize" in {
    serializeTest(numbers.map(_.toString.padTo(8,'\0')), DBString(8))
  }
}

