package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Item(
                 i_item_sk: Long,
                 i_item_id: String,
                 i_rec_start_date: String,
                 i_rec_end_date: String,
                 i_item_desc: String,
                 i_current_price: Double,
                 i_wholesale_cost: Double,
                 i_brand_id: Long,
                 i_brand: String,
                 i_class_id: Long,
                 i_class: String,
                 i_category_id: Long,
                 i_category: String,
                 i_manufact_id: String,
                 i_manufact: String,
                 i_size: String,
                 i_formulation: String,
                 i_color: String,
                 i_units: String,
                 i_container: String,
                 i_manager_id: Long,
                 i_product_name: String
               ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim,
    p(2).trim,
    p(3).trim,
    p(4).trim,
    p(5).trim.toDoubleOrZero,
    p(6).trim.toDoubleOrZero,
    p(7).trim.toLongOrZero,
    p(8).trim,
    p(9).trim.toLongOrZero,
    p(10).trim,
    p(11).trim.toLongOrZero,
    p(12).trim,
    p(13).trim,
    p(14).trim,
    p(15).trim,
    p(16).trim,
    p(17).trim,
    p(18).trim,
    p(19).trim,
    p(20).trim.toLongOrZero,
    p(21).trim
  )
}

object Item extends Table {
  override val name: String = "item"

  override def schema: StructType = StructType(
    Seq(
      StructField("i_item_sk", LongType, nullable = false),
      StructField("i_item_id", StringType, nullable = false),
      StructField("i_rec_start_date", StringType, nullable = true),
      StructField("i_rec_end_date", StringType, nullable = true),
      StructField("i_item_desc", StringType, nullable = true),
      StructField("i_current_price", DoubleType, nullable = true),
      StructField("i_wholesale_cost", DoubleType, nullable = true),
      StructField("i_brand_id", LongType, nullable = true),
      StructField("i_brand", StringType, nullable = true),
      StructField("i_class_id", LongType, nullable = true),
      StructField("i_class", StringType, nullable = true),
      StructField("i_category_id", LongType, nullable = true),
      StructField("i_category", StringType, nullable = true),
      StructField("i_manufact_id", StringType, nullable = true),
      StructField("i_manufact", StringType, nullable = true),
      StructField("i_size", StringType, nullable = true),
      StructField("i_formulation", StringType, nullable = true),
      StructField("i_color", StringType, nullable = true),
      StructField("i_units", StringType, nullable = true),
      StructField("i_container", StringType, nullable = true),
      StructField("i_manager_id", LongType, nullable = true),
      StructField("i_product_name", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Item(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

