package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q22(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val t = inventory.join(item, $"i_item_sk" === $"inv_item_sk")
      .join(warehouse, $"inv_warehouse_sk" === $"w_warehouse_sk")
      .join(dateDim, $"inv_date_sk" === $"d_date_sk" &&
      $"i_current_price" >= 0.98 && $"i_current_price" <= 1.5)
      .withColumn("ddd", lit("2001-05-08").cast("Date"))
      .filter(datediff($"d_date", $"ddd") >= -30 &&
        datediff($"d_date", $"ddd") <= 30)
      .select($"w_warehouse_name", $"i_item_id",
        when((datediff($"d_date", $"ddd") < 0), $"inv_quantity_on_hand").otherwise(0).as("tmp1"),
        when((datediff($"d_date", $"ddd") >= 0), $"inv_quantity_on_hand").otherwise(0).as("tmp2"))
      .groupBy($"w_warehouse_name", $"i_item_id")
      .agg(sum("tmp1").as("inv_before"), sum("tmp2").as("inv_after"))
      .filter($"inv_before" > 0)
      .filter($"inv_after" / $"inv_before" >= 2.0/3.0)
      .filter($"inv_after" / $"inv_before" <= 3.0/2.0)
      .sort($"w_warehouse_name", $"i_item_id")
      .limit(100)

    t

  }
}
