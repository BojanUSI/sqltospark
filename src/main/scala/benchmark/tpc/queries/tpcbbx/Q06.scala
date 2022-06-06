package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset


class Q06(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val yearcond1 = udf { (x: Int, y: Double) => if (x == 2001) y else 0 }
    val yearcond2 = udf { (x: Int, y: Double) => if (x == 2002) y else 0 }

    val store = storeSales.join(dateDim, $"ss_sold_date_sk" === dateDim("d_date_sk") &&
      ($"d_year" === "2001" || $"d_year" === "2002"))
      .select($"ss_customer_sk".as("customer_sk"),
              yearcond1($"d_year", ($"ss_ext_list_price" - $"ss_ext_wholesale_cost" - $"ss_ext_disc_amt" + $"ss_ext_sales_price") / 2).as("fyt"),
              yearcond2($"d_year", ($"ss_ext_list_price" - $"ss_ext_wholesale_cost" - $"ss_ext_disc_amt" + $"ss_ext_sales_price") / 2).as("syt"))
      .groupBy($"customer_sk")
      .agg(sum($"fyt").as("first_year_total"), sum($"syt").as("second_year_total"))
      .filter($"first_year_total" > 0)

    val web = websales.join(dateDim, $"ws_sold_date_sk" === dateDim("d_date_sk") &&
      ($"d_year" === "2001" || $"d_year" === "2002"))
      .select($"ws_bill_customer_sk".as("customer_sk"), yearcond1($"d_year", ($"ws_ext_list_price" - $"ws_ext_wholesale_cost" - $"ws_ext_discount_amt" + $"ws_ext_sales_price") / 2).as("ffyt"),
        yearcond2($"d_year", ($"ws_ext_list_price" - $"ws_ext_wholesale_cost" - $"ws_ext_discount_amt" + $"ws_ext_sales_price") / 2).as("ssyt"))
      .groupBy($"customer_sk")
      .agg(sum($"ffyt").as("first_year_total"), sum($"ssyt").as("second_year_total"))
      .filter($"first_year_total" > 0)

    val res = store.join(web, store("customer_sk") === web("customer_sk"))
      .join(customer, web("customer_sk") === $"c_customer_sk")
      .select((web("second_year_total") / web("first_year_total")).as("web_sales_increase_ratio"),
        $"c_customer_sk", $"c_first_name", $"c_last_name", $"c_preferred_cust_flag", $"c_birth_country", $"c_login",
        $"c_email_address")
      .filter((web("second_year_total") / web("first_year_total")) > (store("second_year_total") / store("first_year_total")))
      .sort($"web_sales_increase_ratio".desc, $"c_customer_sk", $"c_first_name",
        $"c_last_name", $"c_preferred_cust_flag", $"c_birth_country", $"c_login")
      .limit(100)

    res

  }
}
