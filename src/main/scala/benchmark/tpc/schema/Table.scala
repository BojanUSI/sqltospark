package benchmark.tpc.schema

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait Table {
  def name: String

  def schema: StructType

  def convertToDataFrame(spark: SparkSession, rdd: RDD[Array[String]]): DataFrame
}

object Table {
  implicit class StringImplicits(str: String) {
    def toLongOrZero: Long = str.trim match {
      case "" => 0L
      case x  => x.toLong
    }

    def toDoubleOrZero: Double = str.trim match {
      case "" => 0f
      case x  => x.toDouble
    }
  }

}