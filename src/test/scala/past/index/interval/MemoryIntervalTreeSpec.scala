package past.index.interval

import org.scalatest._
import IntervalIndex.Interval

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
}

