import org.apache.spark.sql.SparkSession

object TweetsSQL {
  def main(args: Array[String]): Unit = {
    val jsonFile = "data/sampletweets.json"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-tweets")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
   // tweetsDF.show(100)

    //Show thw scheme of DF
    tweetsDF.printSchema()

    // Register the DataFrame as a SQL temporary view
    tweetsDF.createOrReplaceTempView("tweetTable")
    //Get the most popular languages
    sparkSession.sql(
      " SELECT first(twitter_lang), COUNT(*) as cnt" +
        " FROM tweetTable " +
        " GROUP BY actor.languages ORDER BY cnt DESC LIMIT 25")
      .show(100)
    //Find all the tweets by user
    sparkSession.sql(
      " SELECT actor.displayName, body" +
        " FROM tweetTable " +
        " WHERE  actor.displayName = 'ma-ria '")
      .show(100)
    //Top devices used among all Twitter users
    sparkSession.sql(
      " SELECT generator.displayName, COUNT(*) cnt" +
        " FROM tweetTable " +
        " WHERE  generator.displayName IS NOT NULL" +
        " GROUP BY generator.displayName ORDER BY cnt DESC LIMIT 25")
      .show(100)


  }
}
