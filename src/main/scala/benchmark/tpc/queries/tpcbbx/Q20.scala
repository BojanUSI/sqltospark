package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


class Q20(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._


    var orders = storeSales.select($"ss_customer_sk",
      $"ss_ticket_number", $"ss_item_sk", $"ss_net_paid")
      .groupBy($"ss_customer_sk")
      .agg(
        count_distinct($"ss_ticket_number").as("orders_count"),
        count($"ss_item_sk").as("orders_items"),
        sum($"ss_net_paid").as("orders_money"))

    var returned = storeReturns
      .select($"sr_customer_sk",
        $"sr_ticket_number", $"sr_item_sk", $"sr_return_amt")
      .groupBy($"sr_customer_sk")
      .agg(
        count_distinct($"sr_ticket_number").as("returns_count"),
        count($"sr_item_sk").as("returns_items"),
        sum($"sr_return_amt").as("returns_money"))

    var res = orders.join(returned, $"ss_customer_sk" === $"sr_customer_sk")
      .select(
        $"ss_customer_sk".as("user_sk"),
        round(when(isnull($"returns_count") || isnull($"orders_count")||isnull($"returns_count"/$"orders_count"),0.0).otherwise($"returns_count"/$"orders_count"), 7).as("orderRatio"),
        round(when(isnull($"returns_items") || isnull($"orders_items")||isnull($"returns_items"/$"orders_items"),0.0).otherwise($"returns_items"/$"orders_items"), 7).as("itemsRatio"),
        round(when(isnull($"returns_money") || isnull($"orders_money")||isnull($"returns_money"/$"orders_money"),0.0).otherwise($"returns_money"/$"orders_money"), 7).as("monetaryRatio"),
        round(when(isnull($"returns_count")                                                                       , 0.0).otherwise($"returns_count"), 0).as("frequency"))
      .sort("user_sk")

    res
  }
}
