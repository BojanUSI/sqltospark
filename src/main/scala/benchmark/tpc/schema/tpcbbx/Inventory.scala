package benchmark.tpc.schema.tpcbbx
import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Inventory(
                      inv_date_sk: Long,
                      inv_item_sk: Long,
                      inv_warehouse_sk: Long,
                      inv_quantity_on_hand: Long
                    ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim.toLongOrZero,
    p(2).trim.toLongOrZero,
    p(3).trim.toLongOrZero
  )
}

object Inventory extends Table {
  override val name: String = "inventory"

  override def schema: StructType = StructType(
    Seq(
      StructField("inv_date_sk", LongType, nullable = false),
      StructField("inv_item_sk", LongType, nullable = false),
      StructField("inv_warehouse_sk", LongType, nullable = false),
      StructField("inv_quantity_on_hand", LongType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Inventory(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

