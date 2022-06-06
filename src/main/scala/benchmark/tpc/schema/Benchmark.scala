package benchmark.tpc.schema

import benchmark.tpc.queries.tpcbbx.TPCBBX

sealed trait Benchmark {
  def selector: String

  def tables: Set[Table]

  override def toString: String = getClass.getSimpleName.replaceAll("\\$", "")
}

case object TPCBBX extends Benchmark {
  override val selector: String = classOf[TPCBBX].getName

  override val tables: Set[Table] = Set(
    tpcbbx.Customer,
    tpcbbx.CustomerAddress,
    tpcbbx.CustomerDemographics,
    tpcbbx.DateDim,
    tpcbbx.HouseHoldDemographics,
    tpcbbx.IncomeBand,
    tpcbbx.Inventory,
    tpcbbx.Item,
    tpcbbx.ItemMarketprices,
    tpcbbx.ProductReviews,
    tpcbbx.Promotion,
    tpcbbx.Reason,
    tpcbbx.Shipmode,
    tpcbbx.Store,
    tpcbbx.StoreReturns,
    tpcbbx.StoreSales,
    tpcbbx.TimeDim,
    tpcbbx.Warehouse,
    tpcbbx.WebClickStreams,
    tpcbbx.Webpage,
    tpcbbx.WebReturns,
    tpcbbx.WebSales,
    tpcbbx.Website
  )

}