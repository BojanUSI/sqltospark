package benchmark.tpc.schema.tpcbbx
import benchmark.tpc.schema.Table
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class IncomeBand(
                       ib_income_band_sk: Long
                     ) {
  def this(p: Array[String]) = this(
    p(0).trim.toLong
  )
}

object IncomeBand extends Table {
  override val name: String = "income_band"

  override def schema: StructType = StructType(
    Seq(
      StructField("ib_income_band_sk", LongType, nullable = false)
    )
  )

  override def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame = {
    import spark.implicits._

    val df   = rdd.map(p => new IncomeBand(p)).toDF()
    val cols = df.columns.map(c => col(c).alias(c))
    df.select(cols: _*)

  }
}

