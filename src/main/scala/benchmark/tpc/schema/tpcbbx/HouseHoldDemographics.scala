package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class HouseHoldDemographics(
                                  hd_demo_sk: Long,
                                  hd_income_band_sk: Long,
                                  hd_buy_potential: String,
                                  hd_dep_count: Long,
                                  hd_vehicle_count: Long
                                ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim.toLongOrZero,
    p(2).trim,
    p(3).trim.toLongOrZero,
    p(4).trim.toLongOrZero
  )
}

object HouseHoldDemographics extends Table {
  override val name: String = "household_demographics"

  override def schema: StructType = StructType(
    Seq(
      StructField("hd_demo_sk", LongType, nullable = false),
      StructField("hd_income_band_sk", LongType, nullable = true),
      StructField("hd_buy_potential", StringType, nullable = true),
      StructField("hd_dep_count", LongType, nullable = true),
      StructField("hd_vehicle_count", LongType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new HouseHoldDemographics(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

