package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q12(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond = udf{(x: Double) => if (x >= 37134 && x <= 37134+30) true else false}
    val cond2 = udf{(x: Double) => if (x >= 37134 && x <= 37134+90) true else false}

    val web_in_range = item.join(webClickStreams, cond($"wcs_click_date_sk") &&
      ($"i_category" === "Books" || $"i_category" === "Electronics") && $"wcs_item_sk" === $"i_item_sk"
    && !isnull($"wcs_user_sk") && isnull($"wcs_sales_sk"))
      .select($"wcs_user_sk", $"wcs_click_date_sk")

    val store_in_range = storeSales.join(item, $"ss_item_sk" === $"i_item_sk"
    && !isnull($"ss_customer_sk") && ($"i_category" === "Books" || $"i_category" === "Electronics")
    && cond2($"ss_sold_date_sk"))
      .select($"ss_customer_sk", $"ss_sold_date_sk")

    val res = web_in_range.join(store_in_range,$"wcs_user_sk" === $"ss_customer_sk"
      && $"wcs_click_date_sk" < $"ss_sold_date_sk")
      .select($"wcs_user_sk").distinct()
      .sort($"wcs_user_sk")

    res
  }
}
