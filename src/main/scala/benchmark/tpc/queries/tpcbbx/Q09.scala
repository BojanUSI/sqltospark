package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q09(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond = udf{(x: Double) => if (100 <= x) true else false}
    val cond2 = udf{(x: Double) => if (x <= 150) true else false}
    val cond3 = udf{(x: Double) => if (50 <= x) true else false}
    val cond4 = udf{(x: Double) => if (x <= 150) true else false}
    val cond5 = udf{(x: Double) => if (150 <= x) true else false}
    val cond6 = udf{(x: Double) => if (x <= 200) true else false}
    val cond7 = udf{(x: Double) => if (0 <= x) true else false}
    val cond8 = udf{(x: Double) => if (x <= 2000) true else false}
    val cond9 = udf{(x: Double) => if (x <= 3000) true else false}
    val cond10 = udf{(x: Double) => if (x <= 20000) true else false}

    val r = storeSales.join(dateDim, $"ss_sold_date_sk" === dateDim("d_date_sk")
    && $"d_year" === 2001)
      .join(customerAddress, $"ss_addr_sk" === customerAddress("ca_address_sk"))
      .join(store, $"ss_store_sk" === store("s_store_sk"))
      .join(customerDemographics, $"ss_cdemo_sk" === customerDemographics("cd_demo_sk")
      &&
        (((customerDemographics("cd_marital_status") === "M"
        && customerDemographics("cd_education_status") === "4 yr Degree"
        && cond(storeSales("ss_sales_price"))
        && cond2(storeSales("ss_sales_price")))
      ||
        ((customerDemographics("cd_marital_status") === "M")
          && customerDemographics("cd_education_status") === "4 yr Degree"
          && cond3(storeSales("ss_sales_price"))
          && cond4(storeSales("ss_sales_price")))
      ||
        ((customerDemographics("cd_marital_status") === "M")
          && customerDemographics("cd_education_status") === "4 yr Degree"
          && cond5(storeSales("ss_sales_price"))
          && cond6(storeSales("ss_sales_price"))))
      &&
        (((customerAddress("ca_country") === "United States")
        && (customerAddress("ca_state") === "KY" || customerAddress("ca_state") === "GA" || customerAddress("ca_state") === "NM")
        && cond7(storeSales("ss_net_profit"))
        && cond8(storeSales("ss_net_profit")))
      ||
        ((customerAddress("ca_country") === "United States")
          && (customerAddress("ca_state") === "MT" || customerAddress("ca_state") === "OR" || customerAddress("ca_state") === "IN")
          && cond5(storeSales("ss_net_profit"))
          && cond9(storeSales("ss_net_profit")))
      ||
        ((customerAddress("ca_country") === "United States")
          && (customerAddress("ca_state") === "WI" || customerAddress("ca_state") === "MO" || customerAddress("ca_state") === "WV")
          && cond3(storeSales("ss_net_profit"))
          && cond10(storeSales("ss_net_profit"))))))
      .select(storeSales("ss_quantity").as("ssq"))
      .agg(sum("ssq").as("ss_quantity"))

    r

  }
}
