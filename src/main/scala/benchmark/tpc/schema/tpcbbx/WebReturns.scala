package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class WebReturns(
                       wr_returned_date_sk: Long,
                       wr_returned_time_sk: Long,
                       wr_item_sk: Long,
                       wr_refunded_customer_sk: Long,
                       wr_refunded_cdemo_sk: Long,
                       wr_refunded_hdemo_sk: Long,
                       wr_refunded_addr_sk: Long,
                       wr_returning_customer_sk: Long,
                       wr_returning_cdemo_sk: Long,
                       wr_returning_hdemo_sk: Long,
                       wr_returning_addr_sk: Long,
                       wr_webpage_sk: Long,
                       wr_reason_sk: Long,
                       wr_order_number: Long,
                       wr_return_quantity: Long,
                       wr_return_amt: Double,
                       wr_return_tax: Double,
                       wr_return_amt_inc_tax: Double,
                       wr_fee: Double,
                       wr_return_ship_cost: Double,
                       wr_refunded_cost: Double,
                       wr_reversed_charge: Double,
                       wr_account_credit: Double,
                       wr_net_loss: Double
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
    p(11).trim.toLongOrZero,
    p(12).trim.toLongOrZero,
    p(13).trim.toLongOrZero,
    p(14).trim.toLongOrZero,
    p(15).trim.toDoubleOrZero,
    p(16).trim.toDoubleOrZero,
    p(17).trim.toDoubleOrZero,
    p(18).trim.toDoubleOrZero,
    p(19).trim.toDoubleOrZero,
    p(20).trim.toDoubleOrZero,
    p(21).trim.toDoubleOrZero,
    p(22).trim.toDoubleOrZero,
    p(23).trim.toDoubleOrZero
  )
}

object WebReturns extends Table {
  override val name: String = "web_returns"

  override def schema: StructType = StructType(
    Seq(
      StructField("wr_returned_date_sk", LongType, nullable = false),
      StructField("wr_returned_time_sk", LongType, nullable = false),
      StructField("wr_item_sk", LongType, nullable = true),
      StructField("wr_refunded_customer_sk", LongType, nullable = false),
      StructField("wr_refunded_cdemo_sk", LongType, nullable = false),
      StructField("wr_refunded_hdemo_sk", LongType, nullable = false),
      StructField("wr_refunded_addr_sk", LongType, nullable = false),
      StructField("wr_returning_customer_sk", LongType, nullable = false),
      StructField("wr_returning_cdemo_sk", LongType, nullable = false),
      StructField("wr_returning_hdemo_sk", LongType, nullable = false),
      StructField("wr_returning_addr_sk", LongType, nullable = false),
      StructField("wr_webpage_sk", LongType, nullable = false),
      StructField("wr_reason_sk", LongType, nullable = false),
      StructField("wr_order_number", LongType, nullable = false),
      StructField("wr_return_quantity", LongType, nullable = false),
      StructField("wr_return_amt", DoubleType, nullable = false),
      StructField("wr_return_tax", DoubleType, nullable = false),
      StructField("wr_return_amt_inc_tax", DoubleType, nullable = false),
      StructField("wr_fee", DoubleType, nullable = false),
      StructField("wr_return_ship_cost", DoubleType, nullable = false),
      StructField("wr_refunded_cost", DoubleType, nullable = false),
      StructField("wr_reversed_charge", DoubleType, nullable = false),
      StructField("wr_account_credit", DoubleType, nullable = false),
      StructField("wr_net_loss", DoubleType, nullable = false)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new WebReturns(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

