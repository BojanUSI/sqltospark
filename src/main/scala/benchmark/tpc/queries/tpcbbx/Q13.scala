package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q13(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond = udf{(x: Double, y: Double) => if(x == 2001) y else 0}
    val cond2 = udf{(x: Double, y: Double) => if(x == 2002) y else 0}


    val dd = dateDim.filter($"d_year" === "2001" || $"d_year" === "2002")
      .select($"d_date_sk", $"d_year")

    val store = storeSales.join(dd, $"ss_sold_date_sk" === $"d_date_sk")
      .select($"ss_customer_sk".as("customer_sk"), cond($"d_year", $"ss_net_paid").as("fyt")
      , cond2($"d_year", $"ss_net_paid").as("syt"))
      .groupBy($"customer_sk")
      .agg(sum("fyt").as("first_year_total"), sum("syt").as("second_year_total"))
      .filter($"first_year_total" > 0)

    val web = websales.join(dd, $"ws_sold_date_sk" === $"d_date_sk")
      .select($"ws_bill_customer_sk".as("customer_sk"), cond($"d_year", $"ws_net_paid").as("fyt")
        , cond2($"d_year", $"ws_net_paid").as("syt"))
      .groupBy($"customer_sk")
      .agg(sum("fyt").as("first_year_total"), sum("syt").as("second_year_total"))
      .filter($"first_year_total" > 0)

    val res = store.join(web, store("customer_sk") === web("customer_sk"))
      .join(customer, $"c_customer_sk" === web("customer_sk")
      && ((web("second_year_total") / web("first_year_total")) > (store("second_year_total") / store("second_year_total"))))
      .select($"c_customer_sk", $"c_first_name", $"c_last_name",
        (store("second_year_total") / store("first_year_total")).as("storeSalesIncreaseRatio"),
        (web("second_year_total") / web("first_year_total")).as("webSalesIncreaseRatio"))
      .sort($"webSalesIncreaseRatio".desc, $"c_customer_sk", $"c_first_name", $"c_last_name")
      .limit(100)

    res
  }
}
