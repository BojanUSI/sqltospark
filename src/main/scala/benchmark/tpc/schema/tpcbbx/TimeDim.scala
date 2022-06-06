package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class TimeDim(
                    t_time_sk: Long,
                    t_time_id: String,
                    t_time: Long,
                    t_hour: Long,
                    t_minute: Long,
                    t_second: Long,
                    t_am_pm: String,
                    t_shift: String,
                    t_sub_shift: String,
                    t_mean_shift: String,
                  ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim.toLong,
    p(3).trim.toLong,
    p(4).trim.toLong,
    p(5).trim.toLong,
    p(6).trim,
    p(7).trim,
    p(8).trim,
    p(9).trim
  )
}

object TimeDim extends Table {
  override val name: String = "time_dim"

  override def schema: StructType = StructType(
    Seq(
      StructField("t_time_sk", LongType, nullable = false),
      StructField("t_time_id", StringType, nullable = false),
      StructField("t_time", LongType, nullable = true),
      StructField("t_hour", LongType, nullable = true),
      StructField("t_minute", LongType, nullable = true),
      StructField("t_second", LongType, nullable = true),
      StructField("t_am_pm", StringType, nullable = true),
      StructField("t_shift", StringType, nullable = true),
      StructField("t_sub_shift", StringType, nullable = true),
      StructField("t_mean_shift", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new TimeDim(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

