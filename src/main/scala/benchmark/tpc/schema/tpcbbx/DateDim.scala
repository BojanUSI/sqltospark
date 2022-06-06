package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class DateDim(
                    d_date_sk: Long,
                    d_date_id: String,
                    d_date: String,
                    d_month_seq: Long,
                    d_week_seq: Long,
                    d_quarter_seq: Long,
                    d_year: Long,
                    d_dow: Long,
                    d_moy: Long,
                    d_dom: Long,
                    d_qoy: Long,
                    d_fy_year: Long,
                    d_fy_quarter_seq: Long,
                    d_fy_week_seq: Long,
                    d_day_name: String,
                    d_quarter_name: String,
                    d_holiday: String,
                    d_weekend: String,
                    d_following_holiday: String,
                    d_first_dom: Long,
                    d_last_dom: Long,
                    d_same_day_ly: Long,
                    d_same_day_lq: Long,
                    d_current_day: String,
                    d_current_week: String,
                    d_current_month: String,
                    d_current_quarter: String,
                    d_current_year: String
                  ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim,
    p(3).trim.toLong,
    p(4).trim.toLong,
    p(5).trim.toLong,
    p(6).trim.toLong,
    p(7).trim.toLong,
    p(8).trim.toLong,
    p(9).trim.toLong,
    p(10).trim.toLong,
    p(11).trim.toLong,
    p(12).trim.toLong,
    p(13).trim.toLong,
    p(14).trim,
    p(15).trim,
    p(16).trim,
    p(17).trim,
    p(18).trim,
    p(19).trim.toLong,
    p(20).trim.toLong,
    p(21).trim.toLong,
    p(22).trim.toLong,
    p(23).trim,
    p(24).trim,
    p(25).trim,
    p(26).trim,
    p(27).trim,
  )
}

object DateDim extends Table {
  override val name: String = "date_dim"

  override def schema: StructType = StructType(
    Seq(
      StructField("d_date_sk", LongType, nullable = false),
      StructField("d_date_id", StringType, nullable = false),
      StructField("d_date", StringType, nullable = true),
      StructField("d_month_seq", LongType, nullable = true),
      StructField("d_week_seq", LongType, nullable = true),
      StructField("d_quarter_seq", LongType, nullable = true),
      StructField("d_year", LongType, nullable = true),
      StructField("d_dow", LongType, nullable = true),
      StructField("d_moy", LongType, nullable = true),
      StructField("d_dom", LongType, nullable = true),
      StructField("d_qoy", LongType, nullable = true),
      StructField("d_fy_year", LongType, nullable = true),
      StructField("d_fy_quarter_seq", LongType, nullable = true),
      StructField("d_fy_week_seq", LongType, nullable = true),
      StructField("d_day_name", StringType, nullable = true),
      StructField("d_quarter_name", StringType, nullable = true),
      StructField("d_holiday", StringType, nullable = true),
      StructField("d_weekend", StringType, nullable = true),
      StructField("d_following_holiday", StringType, nullable = true),
      StructField("d_first_dom", LongType, nullable = true),
      StructField("d_last_dom", LongType, nullable = true),
      StructField("d_same_day_ly", LongType, nullable = true),
      StructField("d_same_day_lq", LongType, nullable = true),
      StructField("d_current_day", StringType, nullable = true),
      StructField("d_current_week", StringType, nullable = true),
      StructField("d_current_month", StringType, nullable = true),
      StructField("d_current_quarter", StringType, nullable = true),
      StructField("d_current_year", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new DateDim(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

