package benchmark.modes

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DAT extends ExecutionMode {

  override def fileExtension: String = ".dat"

  override def spark(appName: String, masterUrl: String): SparkSession = getSparkSession(appName, masterUrl)

  override def table(spark: SparkSession, dataFolder: String, table: Table): DataFrame = {
    val split: RDD[Array[String]] = spark.sparkContext
      .textFile(s"${dataFolder}/${table.name}${fileExtension}")
      // .map(_.split('|'))
      .map(_.split("""\|""", -1))

    table.convertToDataFrame(spark, split)
  }
}
