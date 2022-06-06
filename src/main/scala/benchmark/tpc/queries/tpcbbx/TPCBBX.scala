package benchmark.tpc.queries.tpcbbx

import benchmark.modes.{DAT, ExecutionMode, PTXT}
import benchmark.tpc.schema.Table
import benchmark.tpc.schema.tpcbbx._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract  class TPCBBX(spark: SparkSession) {

  var executionMode: ExecutionMode    = _
  var dataFolder: String                = _

  // Add remaining tables here
  lazy val customer: DataFrame = loadDataFrame(Customer)
  lazy val customerAddress: DataFrame = loadDataFrame(CustomerAddress)
  lazy val customerDemographics: DataFrame = loadDataFrame(CustomerDemographics)
  lazy val dateDim: DataFrame = loadDataFrame(DateDim)
  lazy val householdDemographics: DataFrame = loadDataFrame(HouseHoldDemographics)
  //  lazy val incomeBand: DataFrame = loadDataFrame(IncomeBand)
  lazy val inventory: DataFrame = loadDataFrame(Inventory)
  lazy val item: DataFrame = loadDataFrame(Item)
  lazy val itemMarketprices: DataFrame = loadDataFrame(ItemMarketprices)
  lazy val productReviews: DataFrame = loadDataFrame(ProductReviews)
  lazy val promotion: DataFrame = loadDataFrame(Promotion)
  lazy val reason: DataFrame = loadDataFrame(Reason)
  lazy val shipmode: DataFrame = loadDataFrame(Shipmode)
  lazy val store: DataFrame = loadDataFrame(Store)
  lazy val storeReturns: DataFrame = loadDataFrame(StoreReturns)
  lazy val storeSales: DataFrame = loadDataFrame(StoreSales)
  lazy val timeDim: DataFrame = loadDataFrame(TimeDim)
  lazy val warehouse: DataFrame = loadDataFrame(Warehouse)
  lazy val webClickStreams: DataFrame = loadDataFrame(WebClickStreams)
  lazy val webpage: DataFrame = loadDataFrame(Webpage)
  lazy val webreturns: DataFrame = loadDataFrame(WebReturns)
  lazy val websales: DataFrame = loadDataFrame(WebSales)
  lazy val website: DataFrame = loadDataFrame(Website)


  /**
   * The short name of the query
   */
  def shortName: String = getClass.getSimpleName

  /**
   * Gets the header containing the column names from a dataframe
   */
  def getHeader(df: DataFrame): Iterable[String] =
    df.schema.fields.map(field => field.name)

  /**
   * Executes the query
   */
  def execute(): DataFrame

  /**
   * Loads a given table as a DataFrame
   *
   * @param table The table
   * @return The loaded data frame
   */
  def loadDataFrame(table: Table): DataFrame = executionMode.table(spark, dataFolder, table)
}

