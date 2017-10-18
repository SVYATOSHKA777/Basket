import org.apache.spark.sql.types.{FloatType, _}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
class Market(sparkSession: SparkSession, input: String) {

    var MarketSchema = StructType(Array(
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", StringType, true),
      StructField("UnitPrice", DoubleType, true),
      StructField("CustomerID", IntegerType, true),
      StructField("Country", StringType, true)))

    var Market = sparkSession.read
      .option("header", "false")
      .option("delimiter", ";")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(this.MarketSchema)
      .csv(input)

    Market.createOrReplaceTempView("MarketSchema")


    //Find the most mentioned actors (persons)
    def getAll(): DataFrame = {
      sparkSession.sql(
        " SELECT InvoiceNo, StockCode" +
          " FROM MarketSchema"+
          " WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")
    }

  def getdesc(): DataFrame = {
    sparkSession.sql(
      " SELECT StockCode, Description" +
      " FROM MarketSchema"+
      " WHERE StockCode IS NOT NULL AND Description IS NOT NULL")
  }

}
