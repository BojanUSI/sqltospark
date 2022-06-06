package benchmark

import benchmark.LoadTables.{LoadTablesConfig, saveParquet}
import benchmark.modes.{DAT, PTXT}
import benchmark.tpc.queries.tpcbbx.TPCBBX
import benchmark.tpc.schema.{Benchmark, TPCBBX}
import com.jakewharton.fliptables.FlipTable
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.GenSeq

object RunTPCBBXQuery {
  case class ExecConfig(
                         // benchmark whose tables should be converted
                         benchmark: Benchmark = TPCBBX,
                         // folder containing ".ptxt" files
                         inputDirectory: String ,
                         // output folder for result files
                         outputDirectory: String,
                         // master url
                         masterUrl: String = "local[4]",
                         // List of queries to be run
                         queryList: List[Int] = List(5,6,7,9,11,12,13,14,17,20,21,22,23,26),
                         // show or hide result
                         showRes: Int = 0,
                         // enables debug printing
                         debug: Boolean = true
                       ) {
    override def toString: String = {
      s"""Load Tables Config
         |    benchmark: $benchmark
         |    input directory: $inputDirectory
         |    output directory: $outputDirectory
         |    debug: ${if (debug) "enabled" else "disabled"}
         |""".stripMargin
    }
  }

  def parseConfig(args: Array[String]): ExecConfig = {

    if (args.length == 0) {
      println(" Usage eg.: ${SPARK_HOME}/bin/spark-submit" +
        " --class benchmark.RunTPCBBXQuery" +
        " ${HOME_OF_PROJECT}/target/DMhw7-0.0.1-SNAPSHOT.jar" +
        " --absInPath /abs/path/to/ptxtFiles --absOutPath abs/path/to/store/resultFiles --masterUrl local[4] --qList 11 --showRes 1")
      throw  new IllegalArgumentException()
    }

    var absInPathToData = "/abs/path/to/ptxtFiles"
    var absOutPathForResults = "abs/path/to/store/resultFiles"
    var masterUrl = "provide a master url, e.g. local[4]"
    var queryList = List(5,6,7,9,11,12,13,14,17,20,21,22,23,26)
    var showRes = 0
    args.sliding(2, 2).toList.collect {
      case Array("--absInPath", pathToData: String) => {
        absInPathToData = pathToData
      }
      case Array("--absOutPath", pathToData: String) => {
        absOutPathForResults = pathToData
      }
      case Array("--masterUrl", mUrl: String) => {
        masterUrl = mUrl
      }
      case Array("--qList", qList: String) => {
        if (qList.toLowerCase() == "all") {
          queryList = List(5,6,7,9,11,12,13,14,17,20,21,22,23,26)
        }
        else {
          queryList = qList.split(",").map(_.toInt).toList
        }
      }
      case Array("--showRes", sRes: String) => {
        showRes = sRes.toInt
      }
    }
    ExecConfig(TPCBBX, absInPathToData, absOutPathForResults, masterUrl, queryList, showRes)
  }

  def writePtxtTimes(outDir: String, firstResult: Iterable[Row], rawTimes: List[List[Double]], queryPath: String, avgTotalTime: Double, stdTotalTime: Double): Unit = {

    val timesFile  = new File(s"${outDir}/ptxtTIMES_raw.txt")
    val timesStdFile  = new File(s"${outDir}/ptxtTIMES_std.txt")
    val timesAvgFile  = new File(s"${outDir}/ptxtTIMES_avg.txt")
    val outputFile = new File(s"${outDir}/ptxtOUTPUT.txt")
    //        addLinesIfExists(outputFile)

    val bwTimesFile = new BufferedWriter(new FileWriter(timesFile, true))
    rawTimes.foreach(iRun => bwTimesFile.write(f"${queryPath}%s\t\t${iRun(0)}%1.8f\n"))
    bwTimesFile.close()

    val bwTimesAvgFile = new BufferedWriter(new FileWriter(timesAvgFile, true))
    bwTimesAvgFile.write(f"${queryPath}%s\t\t${avgTotalTime}%1.8f\n")
    bwTimesAvgFile.close()

    val bwTimesStdFile = new BufferedWriter(new FileWriter(timesStdFile, true))
    bwTimesStdFile.write(f"${queryPath}%s\t\t${stdTotalTime}%1.8f\n")
    bwTimesStdFile.close()

    val bwOutputFile = new BufferedWriter(new FileWriter(outputFile, true))
    bwOutputFile.write(f"${queryPath}%s\n")
    firstResult.foreach(row => bwOutputFile.write(row.mkString("\t\t") + "\n"))
    bwOutputFile.write("\n\n")
    bwOutputFile.close()
  }

  def execPtxtQueryOnce(query: TPCBBX): (Iterable[Row], Iterable[String], Double) = {
    val startTime = System.nanoTime()
    val df = query.execute()
    val results = query.execute.collect()

    val elapsed = (System.nanoTime() - startTime) / 1000000000d
    (results, query.getHeader(df), elapsed)
  }
  def outputResults(results: Iterable[Row], outHeader: Iterable[String]): String = {
    val values = results.map { row =>
      row.toSeq.map(Option(_).map(_.toString).getOrElse("null")).toArray
    }
    println(outHeader.mkString("|", "|", "\n"))
    println(values.mkString("|", "|", "\n"))
    println("Length of header = " + outHeader.toArray.length + "\t number of values = " + values.toArray.length)
    results.foreach(row => println(row.mkString("\t")))
    FlipTable.of(outHeader.toArray, values.toArray)
  }

  def executePtxtQueries(spark: SparkSession, currQuery: Int, execConfig: ExecConfig) = {
    val packageName = "benchmark.tpc.queries.tpcbbx"
    val queryName = f"Q${currQuery}%02d"
    val queryIns = Class.forName(s"${packageName}.${queryName}").getConstructor(classOf[SparkSession]).newInstance(spark).asInstanceOf[TPCBBX]
    queryIns.executionMode = PTXT
    queryIns.dataFolder = execConfig.inputDirectory
    println("Executing Ptxt Query: " + s"${packageName}${queryName}" + "\n============\n")

    val (outRes, outHeader, timeReq) = execPtxtQueryOnce(queryIns)
    var prettyRes = ""
    if (execConfig.showRes != 0) {
      prettyRes = outputResults(outRes, outHeader)
      println(prettyRes)}
    writePtxtTimes(execConfig.outputDirectory, outRes, List(List(timeReq)), s"${packageName}.${queryName}", timeReq, timeReq)
  }

  def main(args: Array[String]): Unit = {
    val config = parseConfig(args)

    println(config)

    val spark = PTXT.spark("Run TPCBBX queries", config.masterUrl)
    spark

    config.queryList.foreach(currQuery => try {
      executePtxtQueries(spark, currQuery, config)
    }
    catch {
      case someE: Exception => println(someE.printStackTrace())
    }
    )
  }

}
