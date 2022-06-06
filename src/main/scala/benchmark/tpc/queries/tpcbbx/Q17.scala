package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q17(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {

    // NOT COMPLETED
    import spark.implicits._
    val x = customer.join(customerAddress, $"c_current_addr_sk" === $"ca_address_sk" && $"ca_gmt_offset" === -5)
      .select($"c_customer_sk")

    val d = storeSales.join(dateDim, $"ss_sold_date_sk" === $"d_date_sk" && $"d_year" === 2001 && $"d_moy" === 12)
      .join(item, $"ss_item_sk" === $"i_item_sk" && ($"i_category" === "Music" || $"i_category" === "Books"))
      .join(store, $"ss_store_sk" === $"s_store_sk" && $"s_gmt_offset" === -5)
      .join(x, $"ss_customer_sk" === x("c_customer_sk"))
      .join(promotion, $"ss_promo_sk" === $"p_promo_sk")
      .select($"p_channel_email", $"p_channel_dmail", $"p_channel_tv", when(($"p_channel_email" === "Y" || $"p_channel_dmail" === "Y" || $"p_channel_tv" === "Y"),
        $"ss_ext_sales_price").otherwise(0).as("ps"), $"ss_ext_sales_price".as("st"))
      .groupBy($"p_channel_email", $"p_channel_dmail", $"p_channel_tv")
      .agg(sum("ps").as("promotional"), sum("st").as("total"))

    val mul = udf{(x:Double, y: Double) => 100*x/y}

    val res = d.select($"promotional".as("ps"), $"total".as("ts")
    ,when(sum("total") > 0, mul(sum("promotional"), sum("total"))).otherwise(0).as("promo_percent"))
      .agg(sum("ps").as("promotional"), sum("ts").as("total"))
      .orderBy($"promotional", $"total")


    d

  }
}
