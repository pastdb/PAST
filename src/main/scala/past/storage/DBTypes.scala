package past.storage

import java.nio.ByteBuffer
import java.io._

object DBType {

  type TypeName = String

  /**
   * Serialize an object to a Java OutputStream and unserialize to a Java InputStream
   * @param size Size in bytes of the object to serialize
   * @tparam T  Type of the object to serialize
   */
  abstract class DBType[T](val size:Int) {

    private var bytes = new Array[Byte](size) //temporary array needed for conversions

    def serialize(value:T, out:OutputStream) = writeBytes(toByteBuffer(value),out)

    def unserialize(data:InputStream): T = {
      data.read(bytes)
      fromBytes(bytes)
    }

    /*
      methods to be implemented by the subclasses
     */
    protected def toByteBuffer(value:T):ByteBuffer
    protected def fromBytes(data:Array[Byte]):T

    private def writeBytes(v:ByteBuffer,out:OutputStream){
      v.clear
      v.get(bytes)
      out.write(bytes)
    }

  }

  /*
    The following implementations will probably be remplaced by sth more efficient in the future
   */

  object DBInt32 extends DBType[Int](4) {
    def toByteBuffer(value:Int):ByteBuffer = ByteBuffer.allocate(size).putInt(value)
    def fromBytes(data:Array[Byte]) = ByteBuffer.wrap(data).getInt
  }

  object DBInt64 extends DBType[Long](8) {
    def toByteBuffer(value:Long):ByteBuffer = ByteBuffer.allocate(size).putLong(value)
    def fromBytes(data:Array[Byte]) = ByteBuffer.wrap(data).getLong
  }

  object DBFloat32 extends DBType[Float](4) {
    def toByteBuffer(value:Float):ByteBuffer = ByteBuffer.allocate(size).putFloat(value)
    def fromBytes(data:Array[Byte]) = ByteBuffer.wrap(data).getFloat
  }

  object DBFloat64 extends DBType[Double](8) {
    def toByteBuffer(value:Double):ByteBuffer = ByteBuffer.allocate(size).putDouble(value)
    def fromBytes(data:Array[Byte]) = ByteBuffer.wrap(data).getDouble
  }

  case class DBString(val nbChars:Int) extends DBType[String](nbChars) {
    /* in scala Char	16 bit unsigned Unicode character. Range from U+0000 to U+FFFF
        is this the same in Java ? I hope...
     */
    def toByteBuffer(value:String):ByteBuffer = ByteBuffer.allocate(size).put(value.substring(0,Math.min(nbChars,value.length)).getBytes) //or assert ?
    def fromBytes(data:Array[Byte]) = new String(data).toString
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