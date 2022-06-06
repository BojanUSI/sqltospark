package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, substring}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CustomerDemographics(
                                 cd_demo_sk: Long,
                                 cd_gender: String,
                                 cd_marital_status: String,
                                 cd_education_status: String,
                                 cd_purchase_estimate: Long,
                                 cd_credit_rating: String,
                                 cd_dep_count: Long,
                                 cd_dep_employed_count: Long,
                                 cd_dep_college_count: Long
                               ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim,
    p(2).trim,
    p(3).trim,
    p(4).trim.toLongOrZero,
    p(5).trim,
    p(6).trim.toLongOrZero,
    p(7).trim.toLongOrZero,
    p(8).trim.toLongOrZero
  )
}

object CustomerDemographics extends Table {
  override val name: String = "customer_demographics"

  override def schema: StructType = StructType(
    Seq(
      StructField("cd_demo_sk", LongType, nullable = false),
      StructField("cd_gender", StringType, nullable = true),
      StructField("cd_marital_status", StringType, nullable = true),
      StructField("cd_education_status", StringType, nullable = true),
      StructField("cd_purchase_estimate", LongType, nullable = true),
      StructField("cd_credit_rating", StringType, nullable = true),
      StructField("cd_dep_count", LongType, nullable = true),
      StructField("cd_dep_employed_count", LongType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new CustomerDemographics(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

