import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.scalatest._
import past.index.interval.IntervalIndex.Interval
import past.storage.DBType._
import past.storage.{DBType, Schema, Timeseries}
import past.test.util.TestDirectory
import org.apache.spark._
import java.io.{File, FileWriter}
import com.google.common.io.Files
import scala.collection.mutable.ListBuffer

class TimeseriesSpec extends FlatSpec with TestDirectory {

  val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())
  val sc = new SparkContext("local", "Data test")

  trait NameGenerator {
    val name = "pastdb_timeseries_%s".format(System.nanoTime)
  }

  trait Builder extends NameGenerator {
    // TODO: richer schema
    val schema = new Schema(("ts", DBType.DBInt32))
    val timeseries = new Timeseries(name, schema, testDirectory, filesystem)
  }

  "A non-existing Timeseries" should "not exist" in new NameGenerator {
    intercept[java.io.IOException] {
      new Timeseries(name, testDirectory, filesystem)
    }
  }

  it should "be creatable" in new Builder {
    assert(new Timeseries(name, testDirectory, filesystem).schema == schema)
  }

  "Data" should "be able to be inserted and retrieved" in new Builder  {
    val db = new Timeseries(name, testDirectory, filesystem)
    val data = List(1,2,3,4,5,6,7,8,9)
    db.insert(List(("ts",data)))
    db.get[Int](2 to 8,"ts").zip(data.take(8).drop(2).toIterator).foreach{case (x,y) =>
      assert(x == y)
    }
  }

  "Data" should "be able to be manipulated with spark" in new Builder {
    val db = new Timeseries(name, testDirectory, filesystem)
    val data = List(1,2,3,4,5,6,7,8,9)
    db.insertNoSplit(sc,List(("ts", data)))
    val output = db.getRDD[Int](sc,"ts")
    assert(output.collect().toList == data)
  }

  "Data" should "be manipulated with column part files when there's only one file" in new NameGenerator  {
    val schema = new Schema(("ts", DBType.DBInt32), ("data", DBType.DBInt32))
    val db = new Timeseries(name, schema, testDirectory, filesystem)
    val times = List(1,2,3,4,5,6,7,8,9).map(new Integer(_))
    val data = List(10, 11, 12, 13, 14, 15, 16, 17, 18).map(new Integer(_))
    db.insert(sc, times, List(("data", data)))

    val output = db.rangeQueryI32(sc,"data", Interval(2,6))
    assert(output.collect().toList == data.take(5).drop(1))
  }

  "Data" should "be manipulated with column part files when they are split" in new NameGenerator  {
    val schema = new Schema(("ts", DBType.DBInt32), ("data", DBType.DBInt32))
    val db = new Timeseries(name, schema, testDirectory, filesystem,12)
    val times = List(1,2,3,4,5,6,7,8,9).map(new Integer(_))
    val data = List(10, 11, 12, 13, 14, 15, 16, 17, 18).map(new Integer(_))
    db.insert(sc, times, List(("data", data)))

    val output = db.rangeQueryI32(sc,"data", Interval(2,9))
    assert(output.collect().toList == data.take(8).drop(1))
  }

  def dna2Int(c:Char): Integer =
    if (c == 'A') 1
    else if (c == 'D') 2
    else if (c == 'C') -1
    else if (c == 'G') -2
    else 0

  /*"Data" should "be inserted correctly" in new NameGenerator  {
    val file = filesystem.open(new Path("DNA1.txt"))

    val data = new ListBuffer[Integer]()
    val index = new ListBuffer[Integer]()
    var i = 0
    val total = file.available()
    while (file.available() > 0){
      data += dna2Int(file.readByte().toChar)
      index += i
      i += 1
      if (i % 1000000 == 0)
        println(i + " : " + total)
    }
    val schema = new Schema(("ts", DBType.DBInt32),("adn", DBType.DBInt32))
    val db = new Timeseries(name, schema, testDirectory, filesystem,4000000)
    db.insert(sc,index.toList,List(("adn", data.toList)))
    val output = db.rangeQueryI32(sc,"adn", Interval(0,10))
    assert(output.collect().take(10).toList == data.take(10))
  }*/

  /*"Data" should "be inserted correctly with no split" in new NameGenerator  {
    val file = filesystem.open(new Path("DNA1.txt"))

    val data = new ListBuffer[Integer]()
    val index = new ListBuffer[Integer]()
    var i = 0
    val total = file.available()
    while (file.available() > 0){
      data += dna2Int(file.readByte().toChar)
      index += i
      i += 1
      if (i % 1000000 == 0)
        println(i + " : " + total)
    }
    val schema = new Schema(("ts", DBType.DBInt32),("adn", DBType.DBInt32))
    val db = new Timeseries(name, schema, testDirectory, filesystem,4000000)
    db.insertNoSplitListBuffer(sc,List(("ts", index),("adn", data)))
    //val output = db.getRDD[Int](sc,"ts")            #
    val output = db.getRDD[Int](sc, "adn")
    assert(output.collect().take(10).toList == data.take(10).toList)
  }*/

  /*"Data" should "be able to be inserted at different times with spark" in new Builder {
    val db = new Timeseries(name, testDirectory, filesystem)
    val data = List(1,2,3,4,5,6,7,8,9)
    val sc = new SparkContext("local", "Data test")
    db.insertAtColum(sc,"ts",data.take(5))
    db.insertAtColum(sc,"ts",data.drop(5))
    val output = db.getRDD[Int](sc,"ts")
    assert(output.collect().toList === data)
  }   */

  /* "spark" should "work" in new Builder {
     val sc = new SparkContext("local", "test")
     val tempDir = Files.createTempDir()
     val outputDir = new java.io.File(tempDir, "output").getAbsolutePath

    // val outputDir = new java.io.File(testDirectory.toString, "output").getAbsolutePath
     val nums = sc.makeRDD(1 to 4)
     nums.saveAsObjectFile(outputDir.toString)
     // Try reading the output back as an object file
     val output = sc.objectFile[Int](outputDir.toString)
     assert(output.collect().toList === List(1, 2, 3, 4))
   }*/
}
