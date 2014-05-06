package past.index.interval

import org.scalatest._
import IntervalIndex.Interval
import org.apache.hadoop.fs.Path
import java.io.IOException

class MemoryIntervalIndexSpec extends FlatSpec {
  "A IntervalIndex" should "store values" in {
    val index = new MemoryIntervalIndex[Int, Int]()

    index.insert(Interval(0, 2), 0)
    assert(index.get(Interval(0,2)) match {
      case (0, Interval(0,2)) :: Nil => true
      case _ => false
    })

    index.insert(Interval(3, 5), 2)
    assert(index.get(Interval(1, 6)) match {
      case (2, Interval(3, 5)) :: (0, Interval(0,2)) :: Nil => true
      case _ => false
    })
  }
    
  it should "not accept overlapping intervals" in {
    val index = new MemoryIntervalIndex[Int, Int]()
    index.insert(Interval(0,2), 0)

    intercept[IllegalArgumentException] {
      index.insert(Interval(1,2), 0)
    }
    intercept[IllegalArgumentException] {
      index.insert(Interval(-1,2), 0)
    }
  }

  //hopefully temporary
  it should "be able to serialize and unserialize" in {
    val path = new Path("testPath")
    val index = new MemoryIntervalIndex[Int, Int]()
    index.insert(Interval(0,2), 0)
    index.insert(Interval(3, 5), 2)

    MemoryIntervalIndex.store(index, path)

    val unserialized = MemoryIntervalIndex.load[Int,Int](path)

    assert(unserialized.get(Interval(1, 6)) match {
      case (2, Interval(3, 5)) :: (0, Interval(0,2)) :: Nil => true
      case _ => false
    })
  }
}

