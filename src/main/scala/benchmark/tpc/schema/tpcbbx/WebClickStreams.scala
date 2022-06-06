package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class WebClickStreams(
                            wcs_click_sk: Long,
                            wcs_click_date_sk: Long,
                            wcs_click_time_sk: Long,
                            wcs_sales_sk: Long,
                            wcs_item_sk: Long,
                            wcs_web_page_sk: Long,
                            wcs_user_sk: Long
                          ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim.toLongOrZero,
    p(2).trim.toLongOrZero,
    p(3).trim.toLongOrZero,
    p(4).trim.toLongOrZero,
    p(5).trim.toLongOrZero,
    p(5).trim.toLongOrZero
  )
}

object WebClickStreams extends Table {
  override val name: String = "web_clickstreams"

  override def schema: StructType = StructType(
    Seq(
      StructField("wcs_click_sk", LongType, nullable = false),
      StructField("wcs_click_date_sk", LongType, nullable = true),
      StructField("wcs_click_time_sk", LongType, nullable = true),
      StructField("wcs_sales_sk", LongType, nullable = true),
      StructField("wcs_item_sk", LongType, nullable = true),
      StructField("wcs_web_page_sk", LongType, nullable = true),
      StructField("wcs_user_sk", LongType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new WebClickStreams(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

