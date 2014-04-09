package past.storage

import java.nio.ByteBuffer
import java.io._
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

object DBType {

  type TypeName = String

  /**
   * Serialize an object to a Java OutputStream and unserialize to a Java InputStream
   * @param size Size in bytes of the object to serialize
   * @tparam T  Type of the object to serialize
   */
  abstract class DBType[T](val size: Int) {
    def serialize(value: Any, out: FSDataOutputStream) //= writeBytes(toByteBuffer(value), out)
    def unserialize(in: FSDataInputStream): T
  }

  /*
    The following implementations will probably be remplaced by sth more efficient in the future
   */

  object DBInt32 extends DBType[Int](4) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeInt(value.asInstanceOf[Int])
    def unserialize(in: FSDataInputStream): Int = in.readInt()
  }

  object DBInt64 extends DBType[Long](8) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeLong(value.asInstanceOf[Long])
    def unserialize(in: FSDataInputStream): Long = in.readLong()
  }

  object DBFloat32 extends DBType[Float](4) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeFloat(value.asInstanceOf[Float])
    def unserialize(in: FSDataInputStream): Float = in.readFloat()
  }

  object DBFloat64 extends DBType[Double](8) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeDouble(value.asInstanceOf[Double])
    def unserialize(in: FSDataInputStream): Double = in.readDouble()
  }

  case class DBString(val nbChars: Int) extends DBType[String](nbChars) {

    def serialize(value: Any, out: FSDataOutputStream) = {
      val tmp = value.toString
      val withCorrectSize = tmp.substring(0, Math.min(nbChars, tmp.length)).padTo(nbChars,'\0')
      out.writeUTF(withCorrectSize)
    }

    def unserialize(in: FSDataInputStream): String = in.readUTF()
  }

  /*case class DBRecords(fields:List[(TypeName,DBType)]) extends DBType {
    val size = fields.foldLeft(0)((acc,c) => acc + c._2.size)
    def unserialize(data:Stream[Byte]) = {
       fields.foldLeft[List[(TypeName,Any)]](Nil){
         (acc,c) => (c._1,c._2.unserialize(data))::acc
       }
    }
  } */
}