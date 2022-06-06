package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Promotion(
                      p_promo_sk: Long,
                      p_promo_id: String,
                      p_start_date_sk: Long,
                      p_end_date_sk: Long,
                      p_item_sk: Long,
                      p_cost: Double,
                      p_response_target: Long,
                      p_promo_name: String,
                      p_channel_dmail: String,
                      p_channel_email: String,
                      p_channel_catalog: String,
                      p_channel_tv: String,
                      p_channel_radio: String,
                      p_channel_press: String,
                      p_channel_event: String,
                      p_channel_demo: String,
                      p_channel_details: String,
                      p_channel_purpose: String,
                      p_discount_active: String
                    ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim.toLong,
    p(3).trim.toLong,
    p(4).trim.toLong,
    p(5).trim.toDouble,
    p(6).trim.toLong,
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
    p(18).trim
  )
}

object Promotion extends Table {
  override val name: String = "promotion"

  override def schema: StructType = StructType(
    Seq(
      StructField("p_promo_sk", LongType, nullable = false),
      StructField("p_promo_id", StringType, nullable = false),
      StructField("p_start_date_sk", LongType, nullable = true),
      StructField("p_end_date_sk", LongType, nullable = true),
      StructField("p_item_sk", LongType, nullable = true),
      StructField("p_cost", DoubleType, nullable = true),
      StructField("p_response_target", LongType, nullable = true),
      StructField("p_promo_name", StringType, nullable = true),
      StructField("p_channel_dmail", StringType, nullable = true),
      StructField("p_channel_email", StringType, nullable = true),
      StructField("p_channel_catalog", StringType, nullable = true),
      StructField("p_channel_tv", StringType, nullable = true),
      StructField("p_channel_radio", StringType, nullable = true),
      StructField("p_channel_press", StringType, nullable = true),
      StructField("p_channel_event", StringType, nullable = true),
      StructField("p_channel_demo", StringType, nullable = true),
      StructField("p_channel_details", StringType, nullable = true),
      StructField("p_channel_purpose", StringType, nullable = true),
      StructField("p_channel_active", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Promotion(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

