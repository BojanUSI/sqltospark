package benchmark.tpc.queries.tpcbbx

import benchmark.tpc.schema.tpcbbx.CustomerDemographics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q05(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond = udf{(x: String) => if (x == "Movies & TV") 1 else 0}
    val cond1 = udf{(x: Int) => if (x == 1) 1 else 0}
    val cond2 = udf{(x: Int) => if (x == 2) 1 else 0}
    val cond3 = udf{(x: Int) => if (x == 3) 1 else 0}
    val cond4 = udf{(x: Int) => if (x == 4) 1 else 0}
    val cond5 = udf{(x: Int) => if (x == 5) 1 else 0}
    val cond6 = udf{(x: Int) => if (x == 6) 1 else 0}
    val cond7 = udf{(x: Int) => if (x == 7) 1 else 0}

    val condedu = udf{(x: String) => if (x == "Advanced Degree" || x == "College" || x == "4 yr Degree"
      || x == "2 yr Degree") 1 else 0}
    val condgen = udf{(x: String) => if (x == "M") 1 else 0}

    val q05_user_clicks_in_cat = webClickStreams.join(item, $"wcs_item_sk" === item("i_item_sk") && !isnull($"wcs_user_sk"))
      .select($"wcs_user_sk",
        cond($"i_category").as("v0"),
        cond1($"i_category_id").as("v1"),
        cond2($"i_category_id").as("v2"),
        cond3($"i_category_id").as("v3"),
        cond4($"i_category_id").as("v4"),
        cond5($"i_category_id").as("v5"),
        cond6($"i_category_id").as("v6"),
        cond7($"i_category_id").as("v7"))
      .groupBy($"wcs_user_sk")
      .agg(
        sum("v0").as("clicks_in_category"),
        sum("v1").as("clicks_in_1"),
        sum("v2").as("clicks_in_2"),
        sum("v3").as("clicks_in_3"),
        sum("v4").as("clicks_in_4"),
        sum("v5").as("clicks_in_5"),
        sum("v6").as("clicks_in_6"),
        sum("v7").as("clicks_in_7"))

      val res = q05_user_clicks_in_cat.join(customer, $"wcs_user_sk" === customer("c_customer_sk"))
        .join(customerDemographics, $"c_current_cdemo_sk" === $"cd_demo_sk")
        .select($"clicks_in_category",
          condedu($"cd_education_status").as("college_education"), condgen($"cd_gender").as("male"),
        $"clicks_in_1", $"clicks_in_2", $"clicks_in_3", $"clicks_in_4", $"clicks_in_5", $"clicks_in_6", $"clicks_in_7")

    res
  }
}
