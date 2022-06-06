package benchmark.modes

import benchmark.tpc.schema.Table
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ExecutionMode {

  /**
  The File extension of this execution mode. ".dat" for DAT mode, ".ptxt" for PTXT mode
  **/
  def fileExtension: String

  def spark(appName: String = "Unnamed App", masterUrl: String): SparkSession

  /**
  * Fetches a [[DataFrame]] for a given table using a given [[SparkSession]]
  **/

  def table(spark: SparkSession, dataFolder: String, table: Table): DataFrame

  /**
   * Fetches a normal [[SparkSession]]
   * @param appName
   * @return
   */
  def getSparkSession(appName: String, masterUrl: String = "local[4]"): SparkSession = {
    val sparkBuilder = SparkSession
      .builder()
      .appName(appName)
      .master(masterUrl)

    // setup cost-based optimizations
    sparkBuilder.config("spark.sql.cbo.enabled", value = true)

    sparkBuilder.getOrCreate()
  }
}

object ExecutionMode {

  /**
   * Fetches the [[ExecutionMode]] with the given name
   *
   * @param name The name of the execution mode
   * @return The execution mode
   */
  def apply(name: String): ExecutionMode = name.toUpperCase match {
    case "DAT"   => DAT
    case "PTXT"  => PTXT
  }
}
