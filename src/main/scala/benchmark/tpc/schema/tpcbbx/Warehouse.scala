package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Warehouse(
                      w_warehouse_sk: Long,
                      w_warehouse_id: String,
                      w_warehouse_name: String,
                      w_warehouse_sq_ft: Long,
                      w_street_number: String,
                      w_street_name: String,
                      w_street_type: String,
                      w_suite_number: String,
                      w_city: String,
                      w_county: String,
                      w_state: String,
                      w_zip: String,
                      w_country: String,
                      w_gmt_offset: Double
                    ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim,
    p(3).trim.toLong,
    p(4).trim,
    p(5).trim,
    p(6).trim,
    p(7).trim,
    p(8).trim,
    p(9).trim,
    p(10).trim,
    p(11).trim,
    p(12).trim,
    p(13).trim.toDouble
  )
}

object Warehouse extends Table {
  override val name: String = "warehouse"

  override def schema: StructType = StructType(
    Seq(
      StructField("w_warehouse_sk", LongType, nullable = false),
      StructField("w_warehouse_id", StringType, nullable = false),
      StructField("w_warehouse_name", StringType, nullable = true),
      StructField("w_warehouse_sq_ft", LongType, nullable = true),
      StructField("w_street_number", StringType, nullable = true),
      StructField("w_street_name", StringType, nullable = true),
      StructField("w_street_type", StringType, nullable = true),
      StructField("w_suite_number", StringType, nullable = true),
      StructField("w_city", StringType, nullable = true),
      StructField("w_county", StringType, nullable = true),
      StructField("w_state", StringType, nullable = true),
      StructField("w_zip", StringType, nullable = true),
      StructField("w_country", StringType, nullable = true),
      StructField("w_gmt_offset", DoubleType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Warehouse(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

