package benchmark.modes

import benchmark.tpc.schema.Table
import org.apache.spark.sql.{DataFrame, SparkSession}

object PTXT extends  ExecutionMode {
  override def fileExtension: String = ".ptxt"

  override def spark(appName: String, masterUrl: String): SparkSession = getSparkSession(appName, masterUrl)

  override def table(spark: SparkSession, dataFolder: String, table: Table): DataFrame = {
    spark.read
      .schema(table.schema)
      .parquet(s"${dataFolder}/${table.name}${fileExtension}")
  }
}
