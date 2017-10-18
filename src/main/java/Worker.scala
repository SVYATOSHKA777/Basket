import org.apache.spark.sql.SparkSession

object Worker {
  def main(args: Array[String]): Unit = {

    val gdeltFile = "data/gdelt.csv"
    val cameoFile = "data/CAMEO_event_codes.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Initialize GDELT DataFrame
    val gdeltDF = new GDELT(sparkSession, gdeltFile)

    gdeltDF.mostMentionedActors().show(50)
    val mostMentionedEvents = gdeltDF.mostMentionedEvents()
    val cameCodes = sparkSession.read

      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .csv(cameoFile)

    mostMentionedEvents.join(cameCodes,
      mostMentionedEvents.col("EventCode") === cameCodes.col("CAMEOcode"), "inner")
      .select("EventCode", "EventDescription", "count")
      .show(50)
  }
}