package past.storage

import java.io.{FileInputStream, FileOutputStream}
import past.storage.DBType._
import org.scalatest.FlatSpec

/**
 * Created by Eric on 03.04.14.
 */
class DBTypeSpec extends FlatSpec{


  def serializeTest[T](values:List[T],typ:DBType[T],filename:String) = {
    val out = new FileOutputStream(filename)
    values.foreach(x => typ.serialize(x,out))
    out.close
    val in = new FileInputStream(filename)
    values.foreach(x => assert(x == typ.unserialize(in)))
  }

  serializeTest((1 to 100).toList,DBInt32,"filename.txt")
  serializeTest((1 to 100).toList.map(_.toLong),DBInt64,"filename.txt")
  serializeTest((1 to 100).toList.map(_.toFloat),DBFloat32,"filename.txt")
  serializeTest((1 to 100).toList.map(_.toDouble),DBFloat64,"filename.txt")
  serializeTest((1 to 100).toList.map(_.toString.padTo(8,'\0')),DBString(8),"filename.txt")
}
