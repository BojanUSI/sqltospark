package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ItemMarketprices(
                             imp_sk: Long,
                             imp_item_sk: Long,
                             imp_competitor: String,
                             imp_competitor_price: Double,
                             imp_start_date: Long,
                             imp_end_date: Long
                           ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim.toLongOrZero,
    p(2).trim,
    p(3).trim.toDoubleOrZero,
    p(4).trim.toLongOrZero,
    p(5).trim.toLongOrZero
  )
}

object ItemMarketprices extends Table {
  override val name: String = "item_marketprices"

  override def schema: StructType = StructType(
    Seq(
      StructField("imp_sk", LongType, nullable = false),
      StructField("imp_item_sk", LongType, nullable = false),
      StructField("imp_competitor", StringType, nullable = true),
      StructField("imp_competitor_price", DoubleType, nullable = true),
      StructField("imp_start_date", LongType, nullable = true),
      StructField("imp_end_date", LongType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new ItemMarketprices(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

