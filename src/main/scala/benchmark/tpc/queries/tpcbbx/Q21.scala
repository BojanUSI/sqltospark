package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q21(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val part_sr = storeReturns.join(dateDim, $"d_year" === 2003
      && $"d_moy" >= 1 && $"d_moy" <= 6 && $"sr_returned_date_sk" === $"d_date_sk")
      .select($"sr_item_sk", $"sr_customer_sk", $"sr_ticket_number", $"sr_return_quantity")

    val part_ws = websales.join(dateDim, $"ws_sold_date_sk" === $"d_date_sk"
    && $"d_year" >= 2003 && $"d_year" <= 2005)
      .select($"ws_item_sk", $"ws_bill_customer_sk", $"ws_quantity")

    val part_ss = storeSales.join(dateDim, $"d_year" === 2003 && $"d_moy" === 1
    && $"ss_sold_date_sk" === $"d_date_sk")
      .select($"ss_item_sk", $"ss_store_sk", $"ss_customer_sk", $"ss_ticket_number",
      $"ss_quantity")

    val res = part_sr.join(part_ws, $"sr_item_sk" === $"ws_item_sk" && $"sr_customer_sk" === $"ws_bill_customer_sk")
      .join(part_ss, $"ss_ticket_number" === $"sr_ticket_number" && $"ss_item_sk" === $"sr_item_sk"
      && $"ss_customer_sk" === $"sr_customer_sk")
      .join(store, $"s_store_sk" === $"ss_store_sk")
      .join(item, $"i_item_sk" === $"ss_item_sk")
      .select($"i_item_id".as("i_item_id"), $"i_item_desc".as("i_item_desc"),
      $"s_store_id".as("s_store_id"), $"s_store_name".as("s_store_name"),
      $"ss_quantity".as("tmp1"), $"sr_return_quantity".as("tmp2"),
      $"ws_quantity".as("tmp3"))
      .groupBy($"i_item_id", $"i_item_desc", $"s_store_id", $"s_store_name")
      .agg(sum("tmp1").as("store_sales_quantity"),
        sum("tmp2").as("store_returns_quantity"),
        sum("tmp3").as("web_sales_quantity"))
      .orderBy($"i_item_id", $"i_item_desc", $"s_store_id", $"s_store_name")
      .limit(100)

    res
  }
}
