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
import past.index.interval.{MemoryIntervalIndex, IntervalIndex}
import past.index.interval.IntervalIndex.Interval

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
class Timeseries private (name: String,
                          wantedSchema: Schema,
                          containingPath: Path,
                          filesystem: FileSystem,
                          createMode: Boolean) {

  require(Timeseries.isValidName(name), "Timeseries name is not valid")

  type FilesIdentifiers = (List[Int], Int, Int)
  
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
  private val indexPath =  new Path(path, Timeseries.IndexFilename)
  private val dataPath = new Path(path, Timeseries.DataDirName)

  if (!_exists && createMode) {
    filesystem.mkdirs(path)
    filesystem.mkdirs(dataPath)
    wantedSchema.save(schemaPath, filesystem)
    //TODO not use only Int
    MemoryIntervalIndex.store(new MemoryIntervalIndex[Int,Int],indexPath)
  }

  /** The schema of the Timeseries */
  val schema = Schema.load(schemaPath, filesystem)
  //TODO not use only Int but the time type
  val indexes = MemoryIntervalIndex.load[Int,Int](indexPath)

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
    case x => throw new IllegalStateException("Fatal error when acceding file " + column)
  }

  /**
   * Insert data in columns
   * @param sc SparkContext
   * @param times list of new times
   * @param values tuple containing the file names and the data to insert at said file
   */
  def insert[T <% Ordered[T]](sc: SparkContext, times: List[T],values: List[(String, List[_])])(implicit arg0: ClassTag[T]): Unit = {
    assert(values.size + 1 == schema.fields.size)
    values.foreach(s => assert(values.head._2.size == times.size))

    //TODO check all times are sorted ?
    val begin = times.head
    val end = times.last
    assert(begin < end || (begin == end && times.size == 1))
    //indexes.insert(Interval(begin, end))

    insertAtColum(sc,schema.id._1,times)
    values.foreach {c =>
      insertAtColum(sc,c._1,c._2)
    }
  }

	/**
	* Insert data at a certain file
	*
	* @param sc SparkContext
	* @param column file name where the data will be appended
	* @param data raw data to insert
	*/
  private def insertAtColum[T](sc: SparkContext,column:String, data: List[T])(implicit arg0: ClassTag[T]): Unit = insertAtColum(sc,column,sc.makeRDD(data))

	/**
	* Insert data at a certain file
	*
	* @param sc SparkContext
	* @param column file name where the data will be appended
	* @param data an RDD representing the data to be inserted
	*/
  private def insertAtColum[T](sc: SparkContext,column:String, data: RDD[T])(implicit arg0: ClassTag[T]): Unit = schema.fields.get(column) match {
    case Some(typ:DBType[T]) =>
      val outputDir = new java.io.File(dataPath.toString, column).getAbsolutePath
      val onPlaceData = sc.sequenceFile(outputDir, classOf[NullWritable], classOf[BytesWritable], 0)
      onPlaceData.union(
       data.map(x => (NullWritable.get(), new BytesWritable(typ.serialize(x))))
      ).saveAsSequenceFile(outputDir)
    case _ => throw new IllegalArgumentException("Column " + column + " does not exist")
  }

  //TODO take range#
  /**
	* Retrieve file data to a spark RDD
	*
	* @param sc SparkContext
	* @param file file name where to retrieve data
  * @param positions
	*/
  def getRDD[T](sc: SparkContext,file: String, positions: Option[(Int, Int)] = None)(implicit arg0: ClassTag[T]): RDD[T] = schema.fields.get(file) match {
    case Some(typ: DBType[T]) =>
      val outputDir = new java.io.File(dataPath.toString, file).getAbsolutePath
      val rdd = sc.sequenceFile(outputDir, classOf[NullWritable], classOf[BytesWritable], 0)
      (positions match {
        case Some((0, end)) => sc.makeRDD(rdd.take(end))
        case Some((begin, end)) if end < 0 => sc.makeRDD(rdd.toArray.drop(begin))
        case Some((begin, end)) => sc.makeRDD(rdd.take(end).drop(begin))
        case _ => rdd
      }).map(x => typ.unserialize(x._2.getBytes))
    case _ => throw new IllegalArgumentException("Column " + file + " does not exist")
  }

  def getRDDatFiles[T](sc: SparkContext,files: List[String], begin: Int, end: Int)(implicit arg0: ClassTag[T]) = {
    assert(files.size > 0)
    if (files.size == 1) getRDD[T](sc, files.head, Some(begin, end))
    else {
      val start = getRDD[T](sc, files.head, Some(begin, - 1))
      val rdd = files.slice(1, files.size - 1).foldLeft(start){(acc, c) => acc union getRDD[T](sc, c)}
      rdd union getRDD[T](sc, files.last, Some(0, end))
    }
  }

  def getRDDatFiles[T](sc: SparkContext,column: String, identifiers: FilesIdentifiers)(implicit arg0: ClassTag[T]) = {
    val files = identifiers._1.map(column + _)
    getRDDatFiles[T](sc, files, identifiers._2, identifiers._3)
  }

  def getIdentifiersFromInterval(interval: Interval[Int]): FilesIdentifiers = {
    val ids = indexes.get(interval).groupBy(_._1).toList.sortBy(_._1).map {
      case (id, intervals) => (id, (intervals.head._2.start, intervals.last._2.end))
    }
    val tmp = if (ids.size > 2){
      val begin = ids.head
      val middle = ids.slice(1, ids.size - 1).map(x => (x._1, x._2))
      begin :: (middle ++ List(ids.last))
    }
    else
      ids
    (tmp.map(_._1),tmp.head._2._1, tmp.last._2._2)
  }


}

object Timeseries {
  /** The filename of the schema */
  val SchemaFilename = "schema"
  val IndexFilename = "index"
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

