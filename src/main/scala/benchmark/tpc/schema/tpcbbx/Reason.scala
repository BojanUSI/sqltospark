package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Reason(
                   r_reason_sk: Long,
                   r_reason_id: String,
                   r_reason_desc: String
                 ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim
  )
}

object Reason extends Table {
  override val name: String = "reason"

  override def schema: StructType = StructType(
    Seq(
      StructField("r_reason_sk", LongType, nullable = false),
      StructField("r_reason_id", StringType, nullable = false),
      StructField("r_reason_desc", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Reason(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

