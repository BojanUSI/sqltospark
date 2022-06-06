package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Store(
                  s_store_sk: Long,
                  s_store_id: String,
                  s_rec_start_date: String,
                  s_rec_end_date: String,
                  s_closed_date_sk: String,
                  s_store_name: String,
                  s_number_employees: Long,
                  s_floor_space: String,
                  s_hours: String,
                  s_manager: String,
                  s_market_id: String,
                  s_geography_class: String,
                  s_market_desc: String,
                  s_market_manager: String,
                  s_division_id: String,
                  s_division_name: String,
                  s_company_id: String,
                  s_company_name: String,
                  s_street_number: String,
                  s_street_name: String,
                  s_street_type: String,
                  s_suite_number: String,
                  s_city: String,
                  s_county: String,
                  s_state: String,
                  s_zip: String,
                  s_country: String,
                  s_gmt_offset: String
                ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim,
    p(2).trim,
    p(3).trim,
    p(4).trim,
    p(5).trim,
    p(6).trim.toLongOrZero,
    p(7).trim,
    p(8).trim,
    p(9).trim,
    p(10).trim,
    p(11).trim,
    p(12).trim,
    p(13).trim,
    p(14).trim,
    p(15).trim,
    p(16).trim,
    p(17).trim,
    p(18).trim,
    p(19).trim,
    p(20).trim,
    p(21).trim,
    p(22).trim,
    p(23).trim,
    p(24).trim,
    p(25).trim,
    p(26).trim,
    p(27).trim
  )
}

object Store extends Table {
  override val name: String = "store"

  override def schema: StructType = StructType(
    Seq(
      StructField("s_store_sk", LongType, nullable = false),
      StructField("s_store_id", StringType, nullable = false),
      StructField("s_rec_start_date", StringType, nullable = true),
      StructField("s_rec_end_date", StringType, nullable = true),
      StructField("s_closed_date_sk", StringType, nullable = true),
      StructField("s_store_name", StringType, nullable = true),
      StructField("s_number_employees", LongType, nullable = true),
      StructField("s_floor_space", StringType, nullable = true),
      StructField("s_hours", StringType, nullable = true),
      StructField("s_manager", StringType, nullable = true),
      StructField("s_market_id", StringType, nullable = true),
      StructField("s_geography_class", StringType, nullable = true),
      StructField("s_market_desc", StringType, nullable = true),
      StructField("s_market_manager", StringType, nullable = true),
      StructField("s_division_id", StringType, nullable = true),
      StructField("s_division_name", StringType, nullable = true),
      StructField("s_company_id", StringType, nullable = true),
      StructField("s_company_name", StringType, nullable = true),
      StructField("s_street_number", StringType, nullable = true),
      StructField("s_street_name", StringType, nullable = true),
      StructField("s_street_type", StringType, nullable = true),
      StructField("s_suite_number", StringType, nullable = true),
      StructField("s_city", StringType, nullable = true),
      StructField("s_county", StringType, nullable = true),
      StructField("s_state", StringType, nullable = true),
      StructField("s_zip", StringType, nullable = true),
      StructField("s_country", StringType, nullable = true),
      StructField("s_gmt_offset", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Store(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

