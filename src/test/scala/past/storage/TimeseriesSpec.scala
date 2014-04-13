import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest._
import past.storage.DBType._
import past.storage.{DBType, Schema, Timeseries}
import past.test.util.TestDirectory
import org.apache.spark._
import java.io.{File, FileWriter}
import com.google.common.io.Files

class TimeseriesSpec extends FlatSpec with TestDirectory {

  val filesystem = FileSystem.get(new URI("file:///tmp"), new Configuration())

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
    val sc = new SparkContext("local", "Data test")
    db.insertAtColum(sc,"ts",data)
    val output = db.getRDD[Int](sc,"ts")
    assert(output.collect().toList === data)
  }

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
