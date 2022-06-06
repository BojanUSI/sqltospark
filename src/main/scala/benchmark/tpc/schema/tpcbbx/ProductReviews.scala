package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ProductReviews(
                           pr_review_sk: Long,
                           pr_review_date: String,
                           pr_review_time: String,
                           pr_review_rating: Long,
                           pr_item_sk: Long,
                           pr_user_sk: Long,
                           pr_order_sk: Long,
                           pr_review_content: String
                         ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim,
    p(2).trim,
    p(3).trim.toLong,
    p(4).trim.toLong,
    p(5).trim.toLong,
    p(6).trim.toLong,
    p(7).trim
  )
}

object ProductReviews extends Table {
  override val name: String = "product_reviews"

  override def schema: StructType = StructType(
    Seq(
      StructField("pr_review_sk", LongType, nullable = false),
      StructField("pr_review_date", StringType, nullable = true),
      StructField("pr_review_time", StringType, nullable = true),
      StructField("pr_review_rating", LongType, nullable = false),
      StructField("pr_item_sk", LongType, nullable = false),
      StructField("pr_user_sk", LongType, nullable = true),
      StructField("pr_order_sk", LongType, nullable = true),
      StructField("pr_review_content", StringType, nullable = false)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new ProductReviews(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}
