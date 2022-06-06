package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q14(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond = udf{(x: Double) => if (x > 5000 && x < 6000) true else false}
    val cond2 = udf{(x: Double) => if (x == 7 || x == 8 || x == 19 || x == 20) true else false}
    val cond3 = udf{(x: Double) => if (x >= 7 && x <= 8) true else false}
    val cond4 = udf{(x: Double) => if (x >= 19 && x <= 20) true else false}


    val cnt_am_pm = websales.join(householdDemographics, $"hd_demo_sk" === $"ws_ship_hdemo_sk"
    && $"hd_dep_count" === 5)
      .join(webpage, webpage("wp_webpage_sk") === websales("ws_web_page_sk") && cond($"wp_char_count"))
      .join(timeDim, $"t_time_sk" === $"ws_sold_time_sk" && cond2($"t_hour"))
      .select(cond3($"t_hour").as("cnt"), cond4($"t_hour").as("cnt2"))
      .groupBy($"cnt2")
      .agg(count("cnt").as("amc1"), count("cnt2").as("pmc1"))

    val t = cnt_am_pm.select(sum($"amc1").as("amc"), sum($"pmc1").as("pmc"))

    val res = t.select((when($"pmc">0, $"amc").otherwise(-1)).as("am_pm_ratio"))

    res
  }
}
