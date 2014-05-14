package past.storage

import org.apache.hadoop.fs.{FSDataInputStream, FSInputStream, FileSystem, Path}
import past.storage.DBType.{DBInt32, DBType}
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
class Timeseries private (val name: String,
                          wantedSchema: Schema,
                          containingPath: Path,
                          filesystem: FileSystem,
                          createMode: Boolean,
                          maxFileSize: Int = Timeseries.defaultMaxFileSize) {

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
  require(maxFileSize > schema.id._2.size)
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

  def this(name: String, schema: Schema, containingPath: Path, filesystem: FileSystem, maxFileSize: Int) = {
    this(name, schema, containingPath, filesystem, true, maxFileSize)
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
   * @param times list of new values for the time (index) field
   * @param values tuples containing the field name and the data to insert at said field
   * @throws IllegalArgumentException if a time value is already contained in the database
   */
  @throws(classOf[IllegalArgumentException])
  // --> I modify the type of the list 
  //def insert(sc: SparkContext, times: List[int],values: List[(String, List[_])])/*(implicit arg0: ClassTag[T])*/: Unit = {
  def insert(sc: SparkContext, times: List[Integer],values: List[(String, List[Integer])])/*(implicit arg0: ClassTag[T])*/: Unit = {
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
    val nbElemsPerFile = (maxFileSize / fieldSize).toInt
    val data = (schema.id._1, times) :: values

    def getChunkIntervals =  times.splitAt(appendToFileNumber) match {
      case (Nil, r) => r.grouped(nbElemsPerFile).map(c => (c.head, c.last)).toList
      case (l, r) => (l.head, l.last) :: r.grouped(nbElemsPerFile).map(c => (c.head, c.last)).toList
    }

    val intervals = getChunkIntervals

    def chunkData(data: List[(String, List[_])]): List[List[(String, List[_])]] = {
      val tmp = data.map {
         case (n, d) => d.splitAt(appendToFileNumber) match {
          case (Nil, r) => r.grouped(nbElemsPerFile).toList.map((n,_))
          case (l, r) => (n, l) :: r.grouped(nbElemsPerFile).toList.map((n,_))
        }
      }
      (for (j <- 0 until intervals.size) yield (for (i <- 0 until tmp.size) yield tmp(i)(j)).toList).toList
    }

    val chunks = chunkData(data)

    intervals.zip(chunks).zipWithIndex.foreach{
      case ((interval, dat), i ) =>
        indexes.insert(Interval(interval._1, interval._2),i + ident)
        insert(sc, dat, i + ident)
    }
    MemoryIntervalIndex.store(indexes, indexPath)
  }

  @throws(classOf[IllegalArgumentException])
  def insertOld(sc: SparkContext, values: List[(String, List[_])]): Unit = {
    assert(values.size == schema.fields.size)
    values.foreach(s => assert(values.head._2.size == s._2.size))
    values.foreach {c =>
      insertAtColum(sc,c._1,c._2, None)
    }
  }

  @throws(classOf[IllegalArgumentException])
  private def insert(sc: SparkContext, values: List[(String, List[_])], ident: Int): Unit = {
    values.foreach {c =>
      insertAtColum(sc,c._1,c._2, Some(ident))
    }
  }

	/**
	* Insert data at a certain file
	*
	* @param sc SparkContext
	* @param column file name where the data will be appended
	* @param data raw data to insert
	*/
  private def insertAtColum[T](sc: SparkContext,column:String, data: List[T], ident: Option[Int])(implicit arg0: ClassTag[T]): Unit = insertAtColum(sc,column,sc.makeRDD(data), ident)

	/**
	* Insert data at a certain file
	*
	* @param sc SparkContext
	* @param column file name where the data will be appended
	* @param data an RDD representing the data to be inserted
	*/
  private def insertAtColum[T](sc: SparkContext,column:String, data: RDD[T], ident: Option[Int])(implicit arg0: ClassTag[T]): Unit = schema.fields.get(column) match {
    case Some(typ:DBType[T]) =>
      val outputDir = new java.io.File(dataPath.toString, column + ident.getOrElse("")).getAbsolutePath
      val onPlaceData = sc.sequenceFile(outputDir, classOf[NullWritable], classOf[BytesWritable], 0)
      onPlaceData.union(
       data.map(x => (NullWritable.get(), new BytesWritable(typ.serialize(x))))
      ).saveAsSequenceFile(outputDir)
    case _ => throw new IllegalArgumentException("Column " + column + " does not exist")
  }



  /**
	* Retrieve a column file part to a Spark RDD
	* @param sc SparkContext
	* @param column column name where to retrieve data
  * @param positions start and end rows (inclusives) inside the column file where to get the data
  * @param ident column files are separated into several files, each of them have the form /columnNameX where X is
   *              a natural number. ident is this number.
	*/
  def getRDD[T](sc: SparkContext,column: String,
                positions: Option[(Int, Int)] = None,
                ident: Option[Int] = None)(implicit arg0: ClassTag[T]): RDD[T] =
    schema.fields.get(column) match {
    case Some(typ: DBType[T]) =>
      val outputDir = new java.io.File(dataPath.toString, column + ident.getOrElse("")).getAbsolutePath
      val rdd = sc.sequenceFile(outputDir, classOf[NullWritable], classOf[BytesWritable], 0)
      (positions match {
        case Some(interval) =>
          val rdd2 = rdd.map(x => typ.unserialize(x._2.getBytes))
          sc.makeRDD(interval match {
            case (0, end) => rdd2.take(end)
            case (begin, end) if end < 0 => rdd2.toArray.drop(begin)
            case (begin, end) => rdd2.take(end).drop(begin)
          })
        case _ => rdd.map(x => typ.unserialize(x._2.getBytes))
      })
    case _ => throw new IllegalArgumentException("Columnq " + column + " does not exist")
  }

  /**
   *
   * @param sc SparkContext
   * @param column column name
   * @param idents file identifiers on which to read data
   * @param begin begin position in the first column file where to read the data
   * @param end end position in the last column file wher eto read the data
   * @return a Spark RDD
   */
  private def getRDDatFiles[T](sc: SparkContext,column: String, idents: List[Int], begin: Int, end: Int)(implicit arg0: ClassTag[T]): RDD[T] = {
    assert(idents.size > 0)
    if (idents.size == 1)
      getRDD[T](sc, column, Some(begin, end), Some(idents.head))
    else {
      val realEndPart = end - (idents.size - 1) * (maxFileSize / schema.id._2.size).toInt
      val start = getRDD[T](sc, column, Some(begin, - 1), Some(idents.head))
      val rdd = idents.slice(1, idents.size - 1).foldLeft(start){(acc, c) => acc union getRDD[T](sc, column, None, Some(c))}
      rdd union getRDD[T](sc, column, Some(0, realEndPart), Some(idents.last))
    }
  }

  def getRDDatFiles[T](sc: SparkContext,column: String, identifiers: FilesIdentifiers)(implicit arg0: ClassTag[T]): RDD[T] = {
    getRDDatFiles[T](sc, column, identifiers._1, identifiers._2, identifiers._3)
  }

  /**
   * Helper function
   * Retrieve the file Identifiers for a given interval (corresponding to the 'time' field)
   * @param interval The interval where to get the data
   * @return a structure containing file identifier and where to start and where to end to read data
   *         in the data files.
   */
  private def getIdentifiersFromInterval(interval: Interval[Int]): FilesIdentifiers = {
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
    val x = (tmp.map(_._1),Math.max(tmp.head._2._1, interval.start), Math.min(tmp.last._2._2, interval.end))
    /*println(":" + x)
    assert(false)  */
    x
  }

  /**
   * Retrieve the file Identifiers for a given interval (corresponding to the 'time' field)
   * @param sc The SparkContext
   * @param interval The interval where to get the data if it is None then retrieve the entire column
   * @return a structure containing file identifier and where to start and where to end to read data
   *         in the data files.
   */
  def getIdentifiers(sc: SparkContext, interval: Option[Interval[Int]]): FilesIdentifiers = interval match {
    case Some(ival) =>
      val identifiers = getIdentifiersFromInterval(ival)
      val beginInterval = identifiers._2
      val endInterval = identifiers._3
      val firstFile = schema.id._1
      val lastFile = schema.id._1
      var beginPos = -1
      var endPos = -1

      val rdd = if (identifiers._1.size == 1) getRDD[Int](sc,firstFile, None, Some(identifiers._1.head))
                else getRDD[Int](sc,firstFile, None, Some(identifiers._1.head)).union(
                      getRDD[Int](sc,lastFile, None, Some(identifiers._1.last)))
      var pos = 0
      //have to convert to array for now
      rdd.toArray.foreach{ i =>
         if (beginPos < 0 && i >= beginInterval) beginPos = pos
         else if (endPos < 0 && i == endInterval) endPos = pos
         pos += 1
      }
      /*println(identifiers + " : (" + beginPos + ", " + endPos + ")")
      assert(false)     */
      (identifiers._1, beginPos, endPos)
    case None => //special case where we want the full column
      val max = indexes.values(indexes.intervals.head)
      ((0 to max).toList, 0, indexes.intervals.head.end)
  }

  /**
   * Get all value of column 'column' in the range interval (from the index field)
   * @param sc The Spark Context
   * @param column column name
   * @param interval interval in where to get the data
   * @return A Spark RDD
   */
  def rangeQuery[T](sc: SparkContext, column: String, interval: Interval[Int])(implicit arg0: ClassTag[T]): RDD[T] = {
    val id = getIdentifiers(sc, Some(interval))
    getRDDatFiles[T](sc, column, id)
  }

  /**
   * Retrieve the entire content of a column
   * @param sc The Spark Context
   * @param column column name
   * @return A Spark RDD
   */
  def rangeQuery[T](sc: SparkContext, column: String)(implicit arg0: ClassTag[T]): RDD[T] = {
    val id = getIdentifiers(sc, None)
    getRDDatFiles[T](sc, column, id)
  }

  /**
   * Retrieve data from a column using the range information from a FilesIdentifiers structure
   * This is done for optimizing data access for different column value but for the same interval.
   * To retrieve the said interval see getIdentifiers method.
   * @param sc The Spark Context
   * @param column column name
   * @param id
   * @return A Spark RDD
   */
  def rangeQuery[T](sc: SparkContext, column: String, id: FilesIdentifiers)(implicit arg0: ClassTag[T]): RDD[T] =
    getRDDatFiles[T](sc, column, id)


  def rangeQueryI32(sc: SparkContext, column: String, id: FilesIdentifiers): RDD[Int] =  getRDDatFiles[Int](sc, column, id)(ClassTag.Int)
  def rangeQueryI64(sc: SparkContext, column: String, id: FilesIdentifiers): RDD[Long] =  getRDDatFiles[Long](sc, column, id)(ClassTag.Long)
  def rangeQueryF32(sc: SparkContext, column: String, id: FilesIdentifiers): RDD[Float] =  getRDDatFiles[Float](sc, column, id)(ClassTag.Float)
  def rangeQueryF64(sc: SparkContext, column: String, id: FilesIdentifiers): RDD[Double] =  getRDDatFiles[Double](sc, column, id)(ClassTag.Double)

  def rangeQueryI32(sc: SparkContext, column: String): RDD[Int] =  rangeQuery[Int](sc, column)(ClassTag.Int)
  def rangeQueryI64(sc: SparkContext, column: String): RDD[Long] =  rangeQuery[Long](sc, column)(ClassTag.Long)
  def rangeQueryF32(sc: SparkContext, column: String): RDD[Float] =  rangeQuery[Float](sc, column)(ClassTag.Float)
  def rangeQueryF64(sc: SparkContext, column: String): RDD[Double] =  rangeQuery[Double](sc, column)(ClassTag.Double)


}

object Timeseries {
  /** The filename of the schema */
  val SchemaFilename = "schema"
  val IndexFilename = "index"
  /** The directory in which the data will be stored */
  val DataDirName = "data"

  val defaultMaxFileSize = 1000

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

