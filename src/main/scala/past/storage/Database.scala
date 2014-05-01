package past.storage

import com.typesafe.config._
import net.ceedubs.ficus.FicusConfig._
import org.apache.hadoop.fs.{FileSystem, Path}
import past.storage.DBType.DBType
import past.util.RegexUtil._
import scala.collection.mutable

/**
 * Represents a Database. If the database does not exist at the specified path,
 * it gets created.
 * 
 * == Configuration options ==
 * Each database instance takes a set of configuration options.
 * Required options:
 *  - `path`: `String` : the path under which the database will create its files.
 *
 * @constructor
 * @param name the database identifier which has to match [[Database.validNamePattern]].
 * @param filesystem the filesystem on which the database lives.
 * @param conf configuration options for the database.
 */
class Database(name: String, filesystem: FileSystem, conf: Config) {
  require(Database.isValidName(name), "Name is not valid")
  require(conf.hasPath("path"), "path is not in the configuration")

  /** The path of the database which may or may not exist. */
  val path = new Path(conf.as[String]("path"), name)
  /** The path to the Magic file */
  private val magicPath = new Path(path, Database.MagicFileName)
  /** Path to where the time series are stored */
  private val timeseriesPath = new Path(path, Database.TimeseriesDirName)
  /** Map of timeseriesName -> timeSeries */
  private val timeseries = mutable.Map[String, Timeseries]()

  if (Database.exists(name, new Path(conf.as[String]("path")), filesystem)) {
    // create the database
    filesystem.listStatus(timeseriesPath).filter(_.isDir).foreach { f =>
      timeseries += 
        f.getPath.getName ->
        new Timeseries(f.getPath.getName, timeseriesPath, filesystem)
    }
  }
  else {
    filesystem.mkdirs(path)
    // Create the Magic file
    val magic = filesystem.create(magicPath)
    // TODO: write the current version too?
    magic.writeInt(Database.Magic)
    magic.close()

    filesystem.mkdirs(timeseriesPath)
  }

  /** Checks if there is a time series with the given name */
  def hasTimeseries(name: String): Boolean = {
    timeseries.contains(name)
  }

  /** Optionally returns the Timeseries with the given name */
  def getTimeseries(name: String): Option[Timeseries] = {
    timeseries.get(name)
  }

  def getTimeseries: List[String] = {
     timeseries.keys.toList
  }

  /** Creats a timeseries in this database */
  def createTimeseries(name: String, schema: Schema): Boolean = {
    if (hasTimeseries(name)) return false

    timeseries += name -> new Timeseries(name, schema, timeseriesPath, filesystem)
    true
  }
}

object Database {
  val Magic = 1729
  val MagicFileName = "magic"

  val TimeseriesDirName = "timeseries"

  /**
   * The regex for valid database identifiers,
   * which is `[a-zA-Z_][a-zA-Z0-9_]*`
   */
  lazy val validNamePattern = """[a-zA-Z_][a-zA-Z0-9_]*""".r

  /**
   * Returns `true` if `name` is a valid identifier for a database,
   * otherwise false.
   */
  def isValidName(name: String): Boolean = {
    validNamePattern accepts name
  }

  /**
   * Checks if a database exists.
   * @param name The name of the database
   * @param path The path that contains the database
   */
  def exists(name: String, path: Path, filesystem: FileSystem): Boolean = {
    val magicPath = new Path(new Path(path, name), Database.MagicFileName)
    filesystem.exists(magicPath)
    // TODO: Check if the Magic value is correct
  }
}

