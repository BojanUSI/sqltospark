package benchmark.tpc.schema.tpcbbx

import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Website(
                    web_site_sk: Long,
                    web_site_id: String
                  ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong,
    p(1).trim
  )
}

object Website extends Table {
  override val name: String = "web_site"

  override def schema: StructType = StructType(
    Seq(
      StructField("web_site_sk", LongType, nullable = false),
      StructField("web_site_id", StringType, nullable = false)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new Website(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

