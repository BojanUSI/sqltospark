package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class StoreSales(
                       ss_sold_date_sk: Long,
                       ss_sold_time_sk: Long,
                       ss_item_sk: Long,
                       ss_customer_sk: Long,
                       ss_cdemo_sk: Long,
                       ss_hdemo_sk: Long,
                       ss_addr_sk: Long,
                       ss_store_sk: Long,
                       ss_promo_sk: Long,
                       ss_ticket_number: Long,
                       ss_quantity: Double,
                       ss_wholesale_cost: Double,
                       ss_list_price: Double,
                       ss_sales_price: Double,
                       ss_ext_disc_amt: Double,
                       ss_ext_sales_price: Double,
                       ss_ext_wholesale_cost: Double,
                       ss_ext_list_price: Double,
                       ss_ext_tax: Double,
                       ss_coupon_amt: Double,
                       ss_net_paid: Double,
                       ss_net_paid_inc_tax: Double,
                       ss_net_profit: Double
                     ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim.toLong,
    p(2).trim.toLong,
    p(3).trim.toLong,
    p(4).trim.toLongOrZero,
    p(5).trim.toLongOrZero,
    p(6).trim.toLongOrZero,
    p(7).trim.toLong,
    p(8).trim.toLongOrZero,
    p(9).trim.toLong,
    p(10).trim.toLong,
    p(11).trim.toDouble,
    p(12).trim.toDouble,
    p(13).trim.toDouble,
    p(14).trim.toDouble,
    p(15).trim.toDouble,
    p(16).trim.toDouble,
    p(17).trim.toDouble,
    p(18).trim.toDouble,
    p(19).trim.toDouble,
    p(20).trim.toDouble,
    p(21).trim.toDouble,
    p(22).trim.toDouble
  )
}

object StoreSales extends Table {
  override val name: String = "store_sales"

  override def schema: StructType = StructType(
    Seq(
      StructField("ss_sold_date_sk", LongType, nullable = true),
      StructField("ss_sold_time_sk", LongType, nullable = true),
      StructField("ss_item_sk", LongType, nullable = false),
      StructField("ss_customer_sk", LongType, nullable = true),
      StructField("ss_cdemo_sk", LongType, nullable = true),
      StructField("ss_hdemo_sk", LongType, nullable = true),
      StructField("ss_addr_sk", LongType, nullable = true),
      StructField("ss_store_sk", LongType, nullable = true),
      StructField("ss_promo_sk", LongType, nullable = true),
      StructField("ss_ticket_number", LongType, nullable = false),
      StructField("ss_quantity", DoubleType, nullable = true),
      StructField("ss_wholesale_cost", DoubleType, nullable = true),
      StructField("ss_list_price", DoubleType, nullable = true),
      StructField("ss_sales_price", DoubleType, nullable = true),
      StructField("ss_ext_disc_amt", DoubleType, nullable = true),
      StructField("ss_ext_sales_price", DoubleType, nullable = true),
      StructField("ss_ext_wholesale_cost", DoubleType, nullable = true),
      StructField("ss_ext_list_price", DoubleType, nullable = true),
      StructField("ss_ext_tax", DoubleType, nullable = true),
      StructField("ss_coupon_amt", DoubleType, nullable = true),
      StructField("ss_net_paid", DoubleType, nullable = true),
      StructField("ss_net_paid_inc_tax", DoubleType, nullable = true),
      StructField("ss_net_profit", DoubleType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new StoreSales(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

