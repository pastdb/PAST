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
  /** The maximum size the time column file can have (in bytes)*/
  val maxFileSize = 1000
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
  @throws(classOf[IllegalArgumentException])
  def insert(sc: SparkContext, times: List[Int],values: List[(String, List[_])])/*(implicit arg0: ClassTag[T])*/: Unit = {
    assert(values.size + 1 == schema.fields.size)
    values.foreach(s => assert(s._2.size == times.size))

    //TODO check all times are sorted ?
    val begin = times.head
    val end = times.last
    //get the current file identifier
    val ident = if (indexes.intervals.size == 0) 0 else indexes.values(indexes.intervals.head)

    val timeFilePath = new Path(dataPath,schema.id._1 + ident)
    val currentFileSize = if (filesystem.exists(timeFilePath)) filesystem.getContentSummary(timeFilePath).getLength else 0

    val fieldSize = schema.id._2.size
    val insertSize = times.size * fieldSize

    val maxCanAppendSize = maxFileSize - currentFileSize
    val appendToFileSize = Math.min(maxCanAppendSize, insertSize)
    val appendToFileNumber = (appendToFileSize / fieldSize).toInt
    val data = (schema.id._1, times) :: values

    def getChunkIntervals =  times.splitAt(appendToFileNumber) match {
      case (l, r) => (l.head, l.last) :: r.grouped(maxFileSize).map(c => (c.head, c.last)).toList
    }

    val intervals = getChunkIntervals

    def chunkData(data: List[(String, List[_])]): List[List[(String, List[_])]] = {
      val tmp = data.map {
        case (n, d) => d.splitAt(appendToFileNumber) match {
          case (l, r) =>  (n, l) :: r.grouped(maxFileSize).toList.map((n,_))
        }
      }
      (for (i <- 0 until tmp.size) yield (for (j <- 0 until intervals.size) yield tmp(i)(j)).toList).toList
    }

    val chunks = chunkData(data)

    var i = ident
    intervals.zip(chunks).foreach{
      case (interval, dat) =>
        indexes.insert(Interval(begin, end),ident)
        insert(sc, dat, i)
        i += 1
    }
    MemoryIntervalIndex.store(indexes, indexPath)
  }

  @throws(classOf[IllegalArgumentException])
  def insertOld(sc: SparkContext, values: List[(String, List[_])]): Unit = {
    assert(values.size == schema.fields.size)
    values.foreach(s => assert(values.head._2.size == s._2.size))
    values.foreach {c =>
      insertAtColum(sc,c._1,c._2)
    }
  }

  @throws(classOf[IllegalArgumentException])
  private def insert(sc: SparkContext, values: List[(String, List[_])], ident: Int): Unit = {
    values.foreach {c =>
      insertAtColum(sc,c._1 + ident,c._2)
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

  def getRDDatFiles[T](sc: SparkContext,files: List[String], begin: Int, end: Int)(implicit arg0: ClassTag[T]): RDD[T] = {
    assert(files.size > 0)
    if (files.size == 1) getRDD[T](sc, files.head, Some(begin, end))
    else {
      val start = getRDD[T](sc, files.head, Some(begin, - 1))
      val rdd = files.slice(1, files.size - 1).foldLeft(start){(acc, c) => acc union getRDD[T](sc, c)}
      rdd union getRDD[T](sc, files.last, Some(0, end))
    }
  }

  def getRDDatFiles[T](sc: SparkContext,column: String, identifiers: FilesIdentifiers)(implicit arg0: ClassTag[T]): RDD[T] = {
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

  def getIdentifiersFromTime(sc: SparkContext, interval: Interval[Int]): FilesIdentifiers = {
    val identifiers = getIdentifiersFromInterval(interval)
    val beginInterval = identifiers._2
    val endInterval = identifiers._3
    val firstFile = schema.id + identifiers._1.head.toString
    val lastFile = schema.id + identifiers._1.last.toString
    var beginPos = -1
    var endPos = -1
    val rdd = if (firstFile == lastFile) getRDD[Int](sc,firstFile)
              else getRDD[Int](sc,firstFile) union getRDD[Int](sc,lastFile)
    var pos = 0
    rdd.foreach{ i =>
       if (beginPos < 0 && i >= beginInterval) beginPos = pos
       else if (endPos < 0 && i == endInterval) endPos = pos
       pos += 1
    }
    (identifiers._1, beginPos, endPos)
  }

  def rangeQuery[T](sc: SparkContext, interval: Interval[Int], column: String)(implicit arg0: ClassTag[T]): (FilesIdentifiers, RDD[T]) = {
    val id = getIdentifiersFromTime(sc, interval)
    (id, getRDDatFiles[T](sc, column, id))
  }

  def rangeQuery[T](sc: SparkContext, id: FilesIdentifiers, column: String)(implicit arg0: ClassTag[T]): RDD[T] =
    getRDDatFiles[T](sc, column, id)


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

