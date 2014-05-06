package past.index.interval

abstract class IntervalIndex[T <% Ordered[T], V] {
  type Interval = IntervalIndex.Interval[T]
  
  /**
   * Associates a value with an interval. For now, overlapping intervals are
   * not supported.
   *
   * @throws IllegalArgumentException If the interval inserted overlaps with
   * an existing one.
   */
  @throws(classOf[IllegalArgumentException])
  def insert(interval: Interval, value: V): Unit

  /**
   * Returns a list of intervals that overlap with the specified
   * interval along with their associated values.
   */
  def get(interval: Interval): List[(V, Interval)]
}

object IntervalIndex {
  case class Interval[T <% Ordered[T]](val start: T, val end: T)
  extends Ordered[Interval[T]] {
    require(start < end, "start >= end")

    def compare(that: Interval[T]) = start.compare(that.start) match {
      case 0 => end.compare(that.end)
      case c => c
    }

    def overlapsWith(that: Interval[T]): Boolean = {
      ((that.start <= start && start <= that.end)
        || (that.start <= end && end <= that.end)
        || (start <= that.start && that.start <= end)
        || (start <= that.end && that.end <= end))
    }
  }
}

