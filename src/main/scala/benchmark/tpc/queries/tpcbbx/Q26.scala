package benchmark.tpc.queries.tpcbbx

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.Dataset



class Q26(spark: SparkSession) extends TPCBBX(spark) {
  override def execute(): DataFrame = {
    import spark.implicits._

    val cond = udf{(x: String) => if(x == "Books") true else false}

    val cond1 = udf{(x: Long) => if(x == 1) true else false}
    val cond2 = udf{(x: Long) => if(x == 2) true else false}
    val cond3 = udf{(id: Long) => if(id == 3) true else false}
    val cond4 = udf{(id: Long) => if(id == 4) true else false}
    val cond5 = udf{(id: Long) => if(id == 5) true else false}
    val cond6 = udf{(id: Long) => if(id == 6) true else false}
    val cond7 = udf{(id: Long) => if(id == 7) true else false}
    val cond8 = udf{(id: Long) => if(id == 8) true else false}
    val cond9 = udf{(id: Long) => if(id == 9) true else false}
    val cond10 = udf{(id: Long) => if(id == 10) true else false}
    val cond11 = udf{(id: Long) => if(id == 11) true else false}
    val cond12 = udf{(id: Long) => if(id == 12) true else false}
    val cond13 = udf{(id: Long) => if(id == 13) true else false}
    val cond14 = udf{(id: Long) => if(id == 14) true else false}
    val cond15 = udf{(id: Long) => if(id == 15) true else false}



    val res = storeSales.join(item, $"ss_item_sk" === $"i_item_sk"
      && cond($"i_category") && !isnull($"ss_customer_sk"))
      .select($"ss_customer_sk".as("cid"), when(cond1($"i_class_id"), 1).otherwise(null).as("tmp1"),
        when(cond2($"i_class_id"), 1).otherwise(null).as("tmp2"),
        when(cond3($"i_class_id"), 1).otherwise(null).as("tmp3"),
        when(cond4($"i_class_id"), 1).otherwise(null).as("tmp4"),
        when(cond5($"i_class_id"), 1).otherwise(null).as("tmp5"),
        when(cond6($"i_class_id"), 1).otherwise(null).as("tmp6"),
        when(cond7($"i_class_id"), 1).otherwise(null).as("tmp7"),
        when(cond8($"i_class_id"), 1).otherwise(null).as("tmp8"),
        when(cond9($"i_class_id"), 1).otherwise(null).as("tmp9"),
        when(cond10($"i_class_id"), 1).otherwise(null).as("tmp10"),
        when(cond11($"i_class_id"), 1).otherwise(null).as("tmp11"),
        when(cond12($"i_class_id"), 1).otherwise(null).as("tmp12"),
        when(cond13($"i_class_id"), 1).otherwise(null).as("tmp13"),
        when(cond14($"i_class_id"), 1).otherwise(null).as("tmp14"),
        when(cond15($"i_class_id"), 1).otherwise(null).as("tmp15"))
      .groupBy($"cid")
      .agg(count("tmp1").as("id1"),
        count("tmp2").as("id2"),
        count("tmp3").as("id3"),
        count("tmp4").as("id4"),
        count("tmp5").as("id5"),
        count("tmp6").as("id6"),
        count("tmp7").as("id7"),
        count("tmp8").as("id8"),
        count("tmp9").as("id9"),
        count("tmp10").as("id10"),
        count("tmp11").as("id11"),
        count("tmp12").as("id12"),
        count("tmp13").as("id13"),
        count("tmp14").as("id14"),
        count("tmp15").as("id15"))
      .filter(count($"cid") > 5)
      .sort("cid")

    res
  }
}
