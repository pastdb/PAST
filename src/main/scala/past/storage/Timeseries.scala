package past.storage

import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, FileSystem, Path}
import past.storage.DBType.DBType
import javax.security.auth.login.Configuration
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import scala.Some
import org.apache.spark.util.Utils
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/**
 * Represents a time series.
 *
 * @constructor
 * @param name The name of the time series
 * @param wantedSchema The schema to be used if the time series does not exist
 * @param containingPath The path to directory in which the time series will
 * be created
 * @param filesystem The filesystem to use for accessing files
 * @param createMode If it's `true`, the timeseries will be created. If the
 * timeseries alread exists, it will fail with IOException. If it's `false`,
 * an existing timeseries will be opened.
 */
class Timeseries private (name: String, wantedSchema: Schema,
  containingPath: Path, filesystem: FileSystem, createMode: Boolean) {

  require(Timeseries.isValidName(name), "Timeseries name is not valid")

  private val _exists = Timeseries.exists(name, containingPath, filesystem)

  if (_exists && createMode) {
    throw new java.io.IOException("Cannot create existing timeseries")
  }
  else if (!_exists && !createMode) {
    throw new java.io.IOException("Timeseries does not exist")
  }

  /** The path of the Timeseries */
  val path = new Path(containingPath, name)
  private val schemaPath = new Path(path, Timeseries.SchemaFilename)
  private val dataPath = new Path(path, Timeseries.DataDirName)

  if (!_exists && createMode) {
    filesystem.mkdirs(path)
    filesystem.mkdirs(dataPath)
    wantedSchema.save(schemaPath, filesystem)
  }

  /** The schema of the Timeseries */
  val schema = Schema.load(schemaPath, filesystem)

  /**
   * Opens an existing time series.
   */
  def this(name: String, containingPath: Path, filesystem: FileSystem) = {
    this(name, null, containingPath, filesystem, false)
  }

  /**
   * Creates new time series with the given name and stores it in `containingPath`.
   */
  def this(name: String, schema: Schema, containingPath: Path, filesystem: FileSystem) = {
    this(name, schema, containingPath, filesystem, true)
  }

  /*
    for now :
      - values are just un bunch of Any
      - TODO check order of time series:s
  */
  @deprecated("will probably be removed")
  def insert(values : List[(String,List[_])]){
    val data = schema.fields.map(x => values.find(y => x._1 == y._1) match {
      case Some((name,typ)) => (name,typ,x._2)
      case None => throw new IllegalArgumentException(x._1 + "not inserted") //TODO also check type :s
    })

    data.foreach{case (name,data,typ) =>
      val path = new Path(dataPath,name)
      val file = if (!filesystem.exists(path))
                    filesystem.create(path)
                else
                    filesystem.append(path)
      data.foreach{ x =>
        typ.serialize(x,file)
      }
      file.close()
    }
  }

  @deprecated("will probably be removed")
  def get[T](range: Range,column: String):Iterator[T] = schema.fields.get(column) match {
    case None => throw new IllegalArgumentException("Column " + column + " does not exist")
    case Some(typ:DBType[T]) =>
      val path = new Path(dataPath,column)
      if (!filesystem.exists(path))
        throw new IllegalArgumentException("File " + path + " does not exist")
      new Iterator[T] {
        val file = filesystem.open(path)
        val toRead = (range.end - range.start) * typ.size
        var read = 0
        file.seek(range.start * typ.size)

        override def hasNext = read < toRead
        override def next = {
          read += typ.size
          typ.unserialize(file)
        }
        def close = file.close
      }
    case x => throw new IllegalStateException("Fatal error when acceding column " + column)
  }


  def insertAtColum[T](sc: SparkContext,column:String, data: List[T]) = schema.fields.get(column) match {
    case Some(typ:DBType[T]) =>
      val outputDir = new java.io.File(dataPath.toString, column).getAbsolutePath

      val onPlaceData = sc.sequenceFile(outputDir, classOf[NullWritable], classOf[BytesWritable], 0)
      val rdd = sc.makeRDD(data.map(typ.serialize(_)))

      onPlaceData
        .union(rdd
          .map(x => (NullWritable.get(), new BytesWritable(x))))
        .saveAsSequenceFile(outputDir)
    case _ => throw new IllegalArgumentException("Column " + column + " does not exist")
  }

  def getRDD[T](sc: SparkContext,column: String)(implicit arg0: ClassTag[T]): RDD[T] = schema.fields.get(column) match {
    case Some(typ:DBType[T]) =>

      val outputDir = new java.io.File(dataPath.toString, column).getAbsolutePath
      sc.sequenceFile(outputDir, classOf[NullWritable], classOf[BytesWritable], 0).map(x => typ.unserialize(x._2.getBytes))
       /*sc.hadoopFile(path.toString, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],0)//.map(pair => pair._2.toString)
      //sc.hadoopFile[NullWritable,ByteWritable,FileSystem](path.toString)
      TextInputFormat */
    case _ => throw new IllegalArgumentException("Column " + column + " does not exist")
  }

}

object Timeseries {
  /** The filename of the schema */
  val SchemaFilename = "schema"
  /** The directory in which the data will be stored */
  val DataDirName = "data"

  /**
   * Returns `true` if `name` is a valid identifier for a timeseries,
   * otherwise false.
   */
  def isValidName(name: String) = Database.isValidName(name)

  /** Checks if a time series exists */
  def exists(name: String, containingPath: Path, filesystem: FileSystem): Boolean = {
    filesystem.exists(new Path(containingPath, name))
  }
}

