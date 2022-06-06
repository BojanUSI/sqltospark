package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Webpage(
                    wp_webpage_sk: Long,
                    wp_webpage_id: String,
                    wp_rec_start_date: String,
                    wp_rec_end_date: String,
                    wp_creation_date_sk: Long,
                    wp_access_date_sk: Long,
                    wp_autogen_flag: String,
                    wp_customer_sk: Long,
                    wp_url: String,
                    wp_type: String,
                    wp_char_count: Long,
                    wp_link_count: Long,
                    wp_image_count: Long,
                    wp_max_ad_count: Long
                  ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim,
    p(3).trim,
    p(4).trim.toLong,
    p(5).trim.toLong,
    p(6).trim,
    p(7).trim.toLong,
    p(8).trim,
    p(9).trim,
    p(10).trim.toLong,
    p(11).trim.toLong,
    p(12).trim.toLong,
    p(13).trim.toLong
  )
}

object Webpage extends Table {
  override val name: String = "web_page"

  override def schema: StructType = StructType(
    Seq(
      StructField("wp_webpage_sk", LongType, nullable = false),
      StructField("wp_webpage_id", StringType, nullable = false),
      StructField("wp_rec_start_date", StringType, nullable = true),
      StructField("wp_rec_end_date", StringType, nullable = true),
      StructField("wp_creation_date_sk", LongType, nullable = true),
      StructField("wp_access_date_sk", LongType, nullable = true),
      StructField("wp_autogen_flag", StringType, nullable = true),
      StructField("wp_customer_sk", LongType, nullable = true),
      StructField("wp_url", StringType, nullable = true),
      StructField("wp_type", StringType, nullable = true),
      StructField("wp_char_count", LongType, nullable = true),
      StructField("wp_link_count", LongType, nullable = true),
      StructField("wp_image_count", LongType, nullable = true),
      StructField("wp_max_ad_count", LongType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Webpage(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

