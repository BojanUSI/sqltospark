package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class StoreReturns(
                         sr_returned_date_sk: Long,
                         sr_returned_time_sk: Long,
                         sr_item_sk: Long,
                         sr_customer_sk: Long,
                         sr_cdemo_sk: Long,
                         sr_hdemo_sk: Long,
                         sr_addr_sk: Long,
                         sr_store_sk: Long,
                         sr_reason_sk: Long,
                         sr_ticket_number: Long,
                         sr_return_quantity: Long,
                         sr_return_amt: Double,
                         sr_return_tax: Double,
                         sr_return_amt_inc_tax: Double,
                         sr_fee: Double,
                         sr_return_ship_cost: Double,
                         sr_refunded_cash: Double,
                         sr_reversed_charge: Double,
                         sr_store_credit: Double,
                         sr_net_loss: Double
                       ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim.toLongOrZero,
    p(2).trim.toLongOrZero,
    p(3).trim.toLongOrZero,
    p(4).trim.toLongOrZero,
    p(5).trim.toLongOrZero,
    p(6).trim.toLongOrZero,
    p(7).trim.toLongOrZero,
    p(8).trim.toLongOrZero,
    p(9).trim.toLongOrZero,
    p(10).trim.toLongOrZero,
    p(11).trim.toDouble,
    p(12).trim.toDouble,
    p(13).trim.toDouble,
    p(14).trim.toDouble,
    p(15).trim.toDouble,
    p(16).trim.toDouble,
    p(17).trim.toDouble,
    p(18).trim.toDouble,
    p(19).trim.toDouble
  )
}

object StoreReturns extends Table {
  override val name: String = "store_returns"

  override def schema: StructType = StructType(
    Seq(
      StructField("sr_returned_date_sk", LongType, nullable = false),
      StructField("sr_returned_time_sk", LongType, nullable = false),
      StructField("sr_item_sk", LongType, nullable = true),
      StructField("sr_customer_sk", LongType, nullable = false),
      StructField("sr_cdemo_sk", LongType, nullable = false),
      StructField("sr_hdemo_sk", LongType, nullable = false),
      StructField("sr_addr_sk", LongType, nullable = false),
      StructField("sr_store_sk", LongType, nullable = false),
      StructField("sr_reason_sk", LongType, nullable = false),
      StructField("sr_ticket_number", LongType, nullable = false),
      StructField("sr_return_quantity", LongType, nullable = false),
      StructField("sr_return_amt", DoubleType, nullable = false),
      StructField("sr_return_tax", DoubleType, nullable = false),
      StructField("sr_return_amt_inc_tax", DoubleType, nullable = false),
      StructField("sr_fee", DoubleType, nullable = false),
      StructField("sr_return_ship_cost", DoubleType, nullable = false),
      StructField("sr_refunded_cash", DoubleType, nullable = false),
      StructField("sr_reversed_charge", DoubleType, nullable = false),
      StructField("sr_store_credit", DoubleType, nullable = false),
      StructField("sr_net_loss", DoubleType, nullable = false)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new StoreReturns(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

