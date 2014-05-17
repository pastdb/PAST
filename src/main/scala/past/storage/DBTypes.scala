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
  abstract class DBType[T](val size: Int) extends Serializable{

    def serialize(value: Any, out: FSDataOutputStream) //= writeBytes(toByteBuffer(value), out)
    def unserialize(in: FSDataInputStream): T

    def serialize(value: T): Array[Byte]
    def unserialize(in: Array[Byte]): T
  }

  object DBByte extends DBType[Byte](1) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeByte(value.asInstanceOf[Int])
    def unserialize(in: FSDataInputStream): Byte = in.readByte()

    def serialize(value: Byte): Array[Byte] = {
      val array = new Array[Byte](1)
      array(0) = value
      array
    }
    def unserialize(in: Array[Byte]): Byte = in(0)
  }

  /*
    The following implementations will probably be remplaced by sth more efficient in the future
   */

  object DBInt32 extends DBType[Int](4) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeInt(value.asInstanceOf[Int])
    def unserialize(in: FSDataInputStream): Int = in.readInt()

    def serialize(value: Int): Array[Byte] = {
      //ByteBuffer.allocate(size).putInt(value).clear.array().asInstanceOf[Array[Byte]]
      val array = new Array[Byte](4)
      array(0) = (value >>> 24).toByte
      array(1) = (value >>> 16).toByte
      array(2) = (value >>> 8).toByte
      array(3) = (value).toByte
      array
    }

    def unserialize(in: Array[Byte]): Int = ByteBuffer.wrap(in).getInt
  }

  object DBInt64 extends DBType[Long](8) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeLong(value.asInstanceOf[Long])
    def unserialize(in: FSDataInputStream): Long = in.readLong()

    def serialize(value: Long): Array[Byte] =
      ByteBuffer.allocate(size).putLong(value).clear.array().asInstanceOf[Array[Byte]]

    def unserialize(in: Array[Byte]): Long = ByteBuffer.wrap(in).getLong
  }

  object DBFloat32 extends DBType[Float](4) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeFloat(value.asInstanceOf[Float])
    def unserialize(in: FSDataInputStream): Float = in.readFloat()

    def serialize(value: Float): Array[Byte] =
      ByteBuffer.allocate(size).putFloat(value).clear.array().asInstanceOf[Array[Byte]]

    def unserialize(in: Array[Byte]): Float = ByteBuffer.wrap(in).getFloat
  }

  object DBFloat64 extends DBType[Double](8) {
    def serialize(value: Any, out: FSDataOutputStream) = out.writeDouble(value.asInstanceOf[Double])
    def unserialize(in: FSDataInputStream): Double = in.readDouble()

    def serialize(value: Double): Array[Byte] =
      ByteBuffer.allocate(size).putDouble(value).clear.array().asInstanceOf[Array[Byte]]

    def unserialize(in: Array[Byte]): Double = ByteBuffer.wrap(in).getDouble
  }

  case class DBString(val nbChars: Int) extends DBType[String](nbChars) {

    def serialize(value: Any, out: FSDataOutputStream) = {
      val tmp = value.toString
      val withCorrectSize = tmp.substring(0, Math.min(nbChars, tmp.length)).padTo(nbChars,'\0')
      out.writeUTF(withCorrectSize)
    }

    def unserialize(in: FSDataInputStream): String = in.readUTF()

    def serialize(value: String): Array[Byte] = value.substring(0,Math.min(nbChars,value.length)).getBytes

    def unserialize(in: Array[Byte]): String = new String(in).toString
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