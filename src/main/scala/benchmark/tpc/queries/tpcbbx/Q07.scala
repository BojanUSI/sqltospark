package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q07(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val mul = udf { (x: Double) => x * 1.2 }
    val y = udf{(x: String) => x.matches("2004")}
    val m = udf{(x: String) => x.matches("7")}

    val avgCategoryPrice = item.select($"i_category", mul($"i_current_price").as("avg"))
      .groupBy($"i_category")
      .agg(avg("avg").as("avg_price"))

    val dates = dateDim.select($"d_date_sk").filter(y($"d_year")).filter(m($"d_moy"))
    val datesList = dates.map((f=>f.getLong(0))).collect().toList

    val highPriceItems = avgCategoryPrice.join(item, avgCategoryPrice("i_category") === item("i_category") &&
      (item("i_current_price") > avgCategoryPrice("avg_price"))).
      select($"i_item_sk")

    val res = customerAddress.join(customer, $"ca_address_sk" === customer("c_current_addr_sk") && !isnull($"ca_state"))
      .join(storeSales, customer("c_customer_sk") === storeSales("ss_customer_sk"))
      .where($"ss_sold_date_sk".isin(datesList:_*))
      .join(highPriceItems, storeSales("ss_item_sk") === highPriceItems("i_item_sk"))
      .select($"ca_state", $"ca_state".as("cont"))
      .groupBy($"ca_state")
      .agg(count("cont").as("cnt"))
      .sort($"cnt".desc, $"ca_state")
      .filter($"cnt" >= 10)
      .limit(10)

      res
  }
}
