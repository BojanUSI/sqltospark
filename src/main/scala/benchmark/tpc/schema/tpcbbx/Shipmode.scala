package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Shipmode(
  sm_shipmode_sk: Long,
  sm_shipmode_id: String
) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim
  )
}

object Shipmode extends Table {
  override val name: String = "ship_mode"

  override def schema: StructType = StructType(
    Seq(
      StructField("sm_shipmode_sk", LongType, nullable = false),
      StructField("sm_shipmode_id", StringType, nullable = false)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Shipmode(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

