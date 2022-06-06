package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._



class Q23(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val x = inventory.join(dateDim, $"inv_date_sk" === $"d_date_sk" &&
    $"d_year" === 2001 && $"d_moy" >= 1 && $"d_moy" <= 2)
      .select($"inv_warehouse_sk", $"inv_item_sk", $"d_moy",
        $"inv_quantity_on_hand".cast("decimal(15,5)").as("tmp1"),
      $"inv_quantity_on_hand".cast("decimal(15,5)").as("tmp2"))
      .groupBy($"inv_warehouse_sk", $"inv_item_sk", $"d_moy")
      .agg(stddev_samp("tmp1").as("stdev"),
        avg("tmp2").as("mean"))

    val inv1 = x.select($"inv_warehouse_sk", $"inv_item_sk", $"d_moy",
      ($"stdev"/$"mean").cast("decimal(15,5)").as("cov"))
      .filter($"mean" > 0)
      .filter($"stdev"/ $"mean" >= 1.3)


    val r = inv1.as("inv1").join(inv1.as("inv2"), $"inv1.inv_warehouse_sk" === $"inv2.inv_warehouse_sk"
    && $"inv1.inv_item_sk" === $"inv2.inv_item_sk" && $"inv1.d_moy" === 1
    && $"inv2.d_moy" === 2)
      .select($"inv1.inv_warehouse_sk", $"inv1.inv_item_sk", $"inv1.d_moy", $"inv1.cov",
        $"inv2.d_moy", $"inv2.cov")
      .orderBy($"inv1.inv_warehouse_sk", $"inv1.inv_item_sk")

    r
  }
}
