package past.storage

import com.typesafe.config._
import net.ceedubs.ficus.FicusConfig._
import org.apache.hadoop.fs.{FileSystem, Path}
import past.util.RegexUtil._

/**
 * Represents a Database. Note that if the database does not exist, it is not
 * created on object instantiation, `create` has to be called.
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

  /** The path of the database which may or may not exist. */
  val path = new Path(conf.as[String]("path"), name)
  /** The path to the Magic file */
  private val magicPath = new Path(path, Database.MagicFileName)

  /** Creates the database. If it already exists, nothing happens. */
  def create(): Unit = {
    if (exists) return

    filesystem.mkdirs(path)
    // Create the Magic file
    val magic = filesystem.create(magicPath)
    // TODO: write the current version too?
    magic.writeInt(Database.Magic)
    magic.close()
  }
  
  /** Checks if the database exists */
  def exists: Boolean = {
    filesystem.exists(magicPath)
  }
}

object Database {
  val Magic = 1729
  val MagicFileName = "magic"

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
}

