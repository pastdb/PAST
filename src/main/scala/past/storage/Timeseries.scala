package past.storage

import org.apache.hadoop.fs.{FileSystem, Path}
import past.storage.DBType.DBType

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
class Timeseries private (name: String, wantedSchema: Schema,
  containingPath: Path, filesystem: FileSystem, createMode: Boolean) {

  require(Timeseries.isValidName(name), "Timeseries name is not valid")

  private val _exists = Timeseries.exists(name, containingPath, filesystem)

  if (_exists && createMode) {
    throw new java.io.IOException("Cannot create existing timeseries")
  }
  else if (!_exists && !createMode) {
    throw new java.io.IOException("Timeseries does not exist")
  }

  /** The path of the Timeseries */
  val path = new Path(containingPath, name)
  private val schemaPath = new Path(path, Timeseries.SchemaFilename)
  private val dataPath = new Path(containingPath, Timeseries.DataDirName)

  if (!_exists && createMode) {
    filesystem.mkdirs(path)
    filesystem.mkdirs(dataPath)
    wantedSchema.save(schemaPath, filesystem)
  }

  /** The schema of the Timeseries */
  val schema = Schema.load(schemaPath, filesystem)

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
}

object Timeseries {
  /** The filename of the schema */
  val SchemaFilename = "schema"
  /** The directory in which the data will be stored */
  val DataDirName = "data"

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

