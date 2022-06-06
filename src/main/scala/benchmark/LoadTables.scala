package benchmark

import benchmark.modes.PTXT.fileExtension
import benchmark.modes.{DAT, PTXT}
import benchmark.tpc.schema.{Benchmark, TPCBBX, Table}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadTables {

  case class LoadTablesConfig(
                               // benchmark whose tables should be converted
                               benchmark: Benchmark = TPCBBX,
                               // folder containing ".dat" files
                               inputDirectory: Option[String] = None,
                               // output folder for the parquet files
                               outputDirectory: Option[String] = None,
                               // master url
                               masterUrl: String = "local[4]",
                               // enables debug printing
                               debug: Boolean = true
                             ) {
    override def toString: String = {
      s"""Load Tables Config
         |    benchmark: $benchmark
         |    input directory: $inputDirectory
         |    output directory: $outputDirectory
         |    debug: ${if (debug) "enabled" else "disabled"}
         |""".stripMargin
    }
  }
  /**
   * Saves a given table using parquet files
   *
   * @param spark The used spark session
   * @param table The to be saved table
   * @param config The user configuration
   */
  def saveParquet(spark: SparkSession, table: Table, config: LoadTablesConfig): Unit = {
    val df: DataFrame = DAT.table(spark, config.inputDirectory.get, table)

    if (config.debug) {
      df.printSchema()
    }

    // if output directory was not given, set it to the input directory
    val outDir = config.outputDirectory
      .getOrElse(config.inputDirectory.get)

    // write using parquet format
    df.write
      .mode("overwrite")
      .parquet(s"${outDir}/${table.name}${fileExtension}")

    println(s"Finished loading table ${table.name}")
  }

  def parseConfig(args: Array[String]): LoadTablesConfig = {

    if (args.length == 0) {
      println(" Usage eg.: ${SPARK_HOME}/bin/spark-submit" +
        " --class benchmark.LoadTables" +
        " ${HOME_OF_PROJECT}/target/DMhw7-0.0.1-SNAPSHOT.jar" +
        " --absInPath /abs/path/to/datFiles --absOutPath abs/path/to/store/ptxtFiles --masterUrl local[4]")
      throw  new IllegalArgumentException()
    }

    var absInPathToData = "/abs/path/to/datFiles"
    var absOutPathToData = "abs/path/to/store/ptxtFiles"
    var masterUrl = "provide a master url, e.g. local[4]"
    args.sliding(2, 2).toList.collect {
      case Array("--absInPath", pathToData: String) => {
        absInPathToData = pathToData
      }
      case Array("--absOutPath", pathToData: String) => {
        absOutPathToData = pathToData
      }
      case Array("--masterUrl", mUrl: String) => {
        masterUrl = mUrl
      }
    }
    LoadTablesConfig(TPCBBX, Some(absInPathToData), Some(absOutPathToData), masterUrl)
  }

  def main(args: Array[String]): Unit = {

    val config = parseConfig(args)

    println(config)

    val spark = DAT.spark("Load TPCBBX Tables", config.masterUrl)

    config.benchmark.tables.foreach(table => saveParquet(spark, table, config))
  }
}


