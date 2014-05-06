package past.index.interval

import scala.collection.mutable
import org.apache.hadoop.fs.Path
import java.io._
import scala.Some

class MemoryIntervalIndex[T <% Ordered[T], V]() extends IntervalIndex[T, V] {

  val intervals = new mutable.PriorityQueue[Interval]
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

  /**
   * Temporary method to load a Interval from the disk to memory
   * @param path
   */
  //@throws[classOf(IOException)]
  def load[T <% Ordered[T],V](path: Path): MemoryIntervalIndex[T, V] = {
    val fileIn = new FileInputStream(path.toString)
    val in = new ObjectInputStream(fileIn)
    val index = new MemoryIntervalIndex[T,V]()
    index.intervals.enqueue(in.readObject().asInstanceOf[Array[Interval[T]]] : _*)
    index.values ++= (in.readObject().asInstanceOf[mutable.Map[Interval[T], V]])
    in.close()
    fileIn.close()
    index
  }

  def store[T,V](index: MemoryIntervalIndex[T,V], path: Path) = {
    val fileOut = new FileOutputStream(path.toString)
    val out = new ObjectOutputStream(fileOut)
    out.writeObject(index.intervals.toArray)
    out.writeObject(index.values)
    out.close()
    fileOut.close()
  }

}

