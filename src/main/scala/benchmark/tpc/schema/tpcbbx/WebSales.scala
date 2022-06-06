package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class WebSales(
                     ws_sold_date_sk: Long,
                     ws_sold_time_sk: Long,
                     ws_ship_date_sk: Long,
                     ws_item_sk: Long,
                     ws_bill_customer_sk: Long,
                     ws_bill_cdemo_sk: Long,
                     ws_bill_hdemo_sk: Long,
                     ws_bill_addr_sk: Long,
                     ws_ship_customer_sk: Long,
                     ws_ship_cdemo_sk: Long,
                     ws_ship_hdemo_sk: Long,
                     ws_ship_addr_sk: Long,
                     ws_web_page_sk: Long,
                     ws_web_site_sk: Long,
                     ws_ship_mode_sk: Long,
                     ws_warehouse_sk: Long,
                     ws_promo_sk: Long,
                     ws_order_number: Long,
                     ws_quantity: Long,
                     ws_wholesale_cost: Double,
                     ws_list_price: Double,
                     ws_sales_price: Double,
                     ws_ext_discount_amt: Double,
                     ws_ext_sales_price: Double,
                     ws_ext_wholesale_cost: Double,
                     ws_ext_list_price: Double,
                     ws_ext_tax: Double,
                     ws_coupon_amt: Double,
                     ws_ext_ship_cost: Double,
                     ws_net_paid: Double,
                     ws_net_paid_inc_tax: Double,
                     ws_net_paid_inc_ship: Double,
                     ws_net_paid_inc_ship_tax: Double,
                     ws_net_profit: Double
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
    p(15).trim.toLongOrZero,
    p(16).trim.toLongOrZero,
    p(17).trim.toLongOrZero,
    p(18).trim.toLongOrZero,
    p(19).trim.toDoubleOrZero,
    p(20).trim.toDoubleOrZero,
    p(21).trim.toDoubleOrZero,
    p(22).trim.toDoubleOrZero,
    p(23).trim.toDoubleOrZero,
    p(24).trim.toDoubleOrZero,
    p(25).trim.toDoubleOrZero,
    p(26).trim.toDoubleOrZero,
    p(27).trim.toDoubleOrZero,
    p(28).trim.toDoubleOrZero,
    p(29).trim.toDoubleOrZero,
    p(30).trim.toDoubleOrZero,
    p(31).trim.toDoubleOrZero,
    p(32).trim.toDoubleOrZero,
    p(33).trim.toDoubleOrZero,



  )
}

object WebSales extends Table {
  override val name: String = "web_sales"

  override def schema: StructType = StructType(
    Seq(
      StructField("ws_sold_date_sk", LongType, nullable = false),
      StructField("ws_sold_time_sk", LongType, nullable = false),
      StructField("ws_ship_date_sk", LongType, nullable = false),
      StructField("ws_item_sk", LongType, nullable = true),
      StructField("ws_bill_customer_sk", LongType, nullable = false),
      StructField("ws_bill_cdemo_sk", LongType, nullable = false),
      StructField("ws_bill_hdemo_sk", LongType, nullable = false),
      StructField("ws_bill_addr_sk", LongType, nullable = false),
      StructField("ws_ship_customer_sk", LongType, nullable = false),
      StructField("ws_ship_cdemo_sk", LongType, nullable = false),
      StructField("ws_ship_hdemo_sk", LongType, nullable = false),
      StructField("ws_ship_addr_sk", LongType, nullable = false),
      StructField("ws_web_page_sk", LongType, nullable = false),
      StructField("ws_web_site_sk", LongType, nullable = false),
      StructField("ws_ship_mode_sk", LongType, nullable = false),
      StructField("ws_warehouse_sk", LongType, nullable = false),
      StructField("ws_promo_sk", LongType, nullable = false),
      StructField("ws_order_number", LongType, nullable = true),
      StructField("ws_quantity", LongType, nullable = false),
      StructField("ws_wholesale_cost", DoubleType, nullable = false),
      StructField("ws_list_price", DoubleType, nullable = false),
      StructField("ws_sales_price", DoubleType, nullable = false),
      StructField("ws_ext_discount_amt", DoubleType, nullable = false),
      StructField("ws_ext_sales_price", DoubleType, nullable = false),
      StructField("ws_ext_wholesale_cost", DoubleType, nullable = false),
      StructField("ws_ext_list_price", DoubleType, nullable = false),
      StructField("ws_ext_tax", DoubleType, nullable = false),
      StructField("ws_coupon_amt", DoubleType, nullable = false),
      StructField("ws_ext_ship_cost", DoubleType, nullable = false),
      StructField("ws_net_paid", DoubleType, nullable = false),
      StructField("ws_net_paid_inc_tax", DoubleType, nullable = false),
      StructField("ws_net_paid_inc_ship", DoubleType, nullable = false),
      StructField("ws_net_paid_inc_ship_tax", DoubleType, nullable = false),
      StructField("ws_net_profit", DoubleType, nullable = false)


    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new WebSales(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

