package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q11(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond1 = udf { (x: String) => if (x >= "2003-01-02") true else false }
    val cond2 = udf { (x: String) => if (x <= "2003-02-02") true else false }

    val p = productReviews.filter(!isnull($"pr_item_sk"))
      .select($"pr_item_sk", $"pr_item_sk".as("rc"), $"pr_review_rating".as("avgr"))
      .groupBy($"pr_item_sk")
      .agg(count("rc").as("r_count"), avg("avgr").as("avg_rating"))


    val dd = dateDim.filter(cond1($"d_date")).filter( cond2($"d_date"))
      .select($"d_date_sk")

    val s = websales.join(dd, $"ws_sold_date_sk" === dateDim("d_date_sk") && !isnull($"ws_item_sk"))
      .select($"ws_item_sk", $"ws_net_paid".as("rev"))
      .groupBy($"ws_item_sk")
      .agg(sum("rev").as("revenue"))

    val tmp = p.join(s, p("pr_item_sk") === s("ws_item_sk"))
      .select(p("pr_item_sk").as("pid"), p("r_count").as("reviews_count"), p("avg_rating").as("avg_rating"), s("revenue").as("m_revenue"))

    val q11_review_stats = tmp.select(corr("reviews_count", "avg_rating"))

    q11_review_stats
  }
}
