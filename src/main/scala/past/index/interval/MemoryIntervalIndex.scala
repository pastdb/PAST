package past.index.interval

import scala.collection.mutable

class MemoryIntervalIndex[T <% Ordered[T], V] extends IntervalIndex[T, V] {
  val intervals = new mutable.PriorityQueue[Interval]()
  val values = mutable.Map[Interval, V]()

  @throws(classOf[IllegalArgumentException])
  override def insert(interval: Interval, value: V): Unit =
    intervals.find(_.overlapsWith(interval)) match {
      case Some(i) =>
        throw new IllegalArgumentException("interval overlaps with " + i)
      case None =>
        intervals += interval
        values += { (interval, value) }
    }

  override def get(interval: Interval): List[(V, Interval)] =
    intervals.filter(_.overlapsWith(interval)).toList.map(i => (values.get(i).get, i))
}

object MemoryIntervalIndex {
  type Interval[T] = IntervalIndex.Interval[T]
}

