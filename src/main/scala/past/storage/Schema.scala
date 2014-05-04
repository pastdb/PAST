package past.storage

import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, FSDataOutputStream, Path}
import past.storage.DBType._
import scala.collection.mutable

/**
 * Represents the schema of a time series. On creation, it performs
 * sanity checks on the fields and provides (de-)serialization methods.
 *
 * @constructor
 * @param id the primary timestamp key in this schema
 * @param restFields the rest of the fields that compose the schema
 */
class Schema(val id: Schema.Field, restFields: Schema.Field*) extends Equals {
  if (!restFields.isEmpty) {
    val duplicates = (id :: restFields.toList).groupBy(_._1).filter(_._2.size > 1).keys
    require(duplicates.isEmpty,
      "Duplicate field(s): %s".format(duplicates.mkString(", ")))
  }

  require(id._2 match {
    case DBType.DBInt32 | DBType.DBInt64 | DBType.DBFloat32 | DBType.DBFloat64 => true
    case _ => false },
    "The id field should have numeric type")

  /** The fields of the schema, including the id */
  val fields = Map(id :: restFields.toList: _*)

  override def canEqual(that: Any) = that match {
    case s: Schema => id == s.id && fields == s.fields
    case _ => false
  }

  override def toString(): String = {
    "Schema(%s)".format(fields.map({ case (n,t) => "%s: %s".format(n,t) }).mkString(", "))
  }

  override def hashCode: Int =
    fields.hashCode * 41 + id.hashCode

  override def equals(that: Any) = that match {
    case s: Schema => canEqual(that)
    case _ => false
  }

  /** Saves the schema to `output` */
  def save(output: FSDataOutputStream): Unit = {
    fields foreach { case (name, ftype) =>
      val s = "%s%s%s".format(
        name.toString, Schema.FieldSeparator, ftype.getClass.getName)
      output.writeUTF(s)
    }
  }

  /** Saves the schema to `path` in `filesystem` */
  def save(path: Path, filesystem: FileSystem): Unit = {
    val f = filesystem.create(path)
    this.save(f)
    f.close()
  }
}

object Schema {
  type Field = (String, DBType[_])

  // the name - type separator used in storing field info
  private val FieldSeparator = "\0"

  /**
   * An iterator over the UTF strings of `input`. It will return
   * strings read by `input.readUTF()` until EOF.
   */
  // TODO: move this to `past.util`
  private class UTFIterator(val input: FSDataInputStream) extends Iterator[String] {
    var nextStr: Option[String] = null

    def readStr(): Option[String] = 
      try {
        Some(input.readUTF())
      } catch {
        case exc: java.io.EOFException => None
      }

    override def hasNext: Boolean = {
      nextStr = readStr()
      nextStr.nonEmpty
    }

    override def next(): String = nextStr.get
  }

  /** Loads a schema from `input` */
  // TODO: refactor this to a constructor?
  def load(input: FSDataInputStream): Schema = {
    val fields = new UTFIterator(input).map({ str =>
      val fieldParts = str.split(FieldSeparator)
      // get the object
      val clazz = Class.forName(fieldParts(1))
      val obj = clazz.getField("MODULE$").get(clazz)
      fieldParts(0) -> obj.asInstanceOf[DBType[_]]
    }).toList
    new Schema(fields.head, fields.tail: _*)
  }

  /** Loads a schema from `path` in `filesystem` */
  // TODO: refactor this to a constructor?
  def load(path: Path, filesystem: FileSystem): Schema = {
    val f = filesystem.open(path)
    val schema = Schema.load(f)
    f.close()
    schema
  }
}

