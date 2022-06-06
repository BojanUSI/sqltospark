package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CustomerAddress(
                     ca_address_sk: Long,
                     ca_address_id: String,
                     ca_street_number: String,
                     ca_street_name: String,
                     ca_street_type: String,
                     ca_suite_number: String,
                     ca_city: String,
                     ca_county: String,
                     ca_state: String,
                     ca_zip: String,
                     ca_country: String,
                     ca_gmt_offset: Double,
                     ca_location_type: String
                   ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim,
    p(3).trim,
    p(4).trim,
    p(5).trim,
    p(6).trim,
    p(7).trim,
    p(8).trim,
    p(9).trim,
    p(10).trim,
    p(11).trim.toDouble,
    p(12).trim
  )
}

object CustomerAddress extends Table {
  override val name: String = "customer_address"

  override def schema: StructType = StructType(
    Seq(
      StructField("ca_address_sk", LongType, nullable = false),
      StructField("ca_address_id", StringType, nullable = false),
      StructField("ca_street_number", StringType, nullable = true),
      StructField("ca_street_name", StringType, nullable = true),
      StructField("ca_street_type", StringType, nullable = true),
      StructField("ca_suite_number", StringType, nullable = true),
      StructField("ca_city", StringType, nullable = true),
      StructField("ca_county", StringType, nullable = true),
      StructField("ca_state", StringType, nullable = true),
      StructField("ca_zip", StringType, nullable = true),
      StructField("ca_country", StringType, nullable = true),
      StructField("ca_gmt_offset", DoubleType, nullable = true),
      StructField("ca_location_type", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new CustomerAddress(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

