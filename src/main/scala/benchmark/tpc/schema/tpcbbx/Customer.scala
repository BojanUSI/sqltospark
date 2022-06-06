package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import benchmark.tpc.schema.Table.StringImplicits
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Customer(
                   c_customer_sk: Long,
                   c_customer_id: String,
                   c_current_cdemo_sk: Long,
                   c_current_hdemo_sk: Long,
                   c_current_addr_sk: Long,
                   c_first_shipto_date_sk: Long,
                   c_first_sales_date_sk: Long,
                   c_salutation: String,
                   c_first_name: String,
                   c_last_name: String,
                   c_preferred_cust_flag: String,
                   c_birth_day: Long,
                   c_birth_month: Long,
                   c_birth_year: Long,
                   c_birth_country: String,
                   c_login: String,
                   c_email_address: String,
                   c_last_review_date: String
                   ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLongOrZero,
    p(1).trim,
    p(2).trim.toLongOrZero,
    p(3).trim.toLongOrZero,
    p(4).trim.toLongOrZero,
    p(5).trim.toLongOrZero,
    p(6).trim.toLongOrZero,
    p(7).trim,
    p(8).trim,
    p(9).trim,
    p(10).trim,
    p(11).trim.toLongOrZero,
    p(12).trim.toLongOrZero,
    p(13).trim.toLongOrZero,
    p(14).trim,
    p(15).trim,
    p(16).trim,
    p(17).trim
  )
}

object Customer extends Table {
  override val name: String = "customer"

  override def schema: StructType = StructType(
    Seq(
      StructField("c_customer_sk", LongType, nullable = false),
      StructField("c_customer_id", StringType, nullable = false),
      StructField("c_current_cdemo_sk", LongType, nullable = true),
      StructField("c_current_hdemo_sk", LongType, nullable = true),
      StructField("c_current_addr_sk", LongType, nullable = true),
      StructField("c_first_shipto_date_sk", LongType, nullable = true),
      StructField("c_first_sales_date_sk", LongType, nullable = true),
      StructField("c_salutation", StringType, nullable = true),
      StructField("c_first_name", StringType, nullable = true),
      StructField("c_last_name", StringType, nullable = true),
      StructField("c_preferred_cust_flag", StringType, nullable = true),
      StructField("c_birth_day", LongType, nullable = true),
      StructField("c_birth_month", LongType, nullable = true),
      StructField("c_birth_year", LongType, nullable = true),
      StructField("c_birth_country", StringType, nullable = true),
      StructField("c_login", StringType, nullable = true),
      StructField("c_email_address", StringType, nullable = true),
      StructField("c_last_review_date", StringType, nullable = true)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Customer(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}
