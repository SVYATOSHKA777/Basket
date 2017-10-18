import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object MarketBasketAnalysis {

    def main(args: Array[String]): Unit = {
      val inputfile = "data/Online Retail.csv"

      val ss = SparkSession
        .builder()
        .appName("spark-sql-basic")
        .master("local[*]")
        .getOrCreate()

     val DataSetSQL = new Market(ss,inputfile)
      val dataFrame = DataSetSQL.getAll()
      val keyvalue = dataFrame.rdd.map(row =>(row(0),row(1).toString))
      val groupedkeyvalue = keyvalue.groupByKey()
      val transactions = groupedkeyvalue.map(row => row._2.toArray.distinct)

      val fpg = new FPGrowth()
        .setMinSupport(0.02)

      val model = fpg.run(transactions)

      model.freqItemsets.collect().foreach { itemset =>
        println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
      }

      //get association rules
      val minConfidence = 0.01
      val rules = model.generateAssociationRules(minConfidence)

      val dataDesc = DataSetSQL.getdesc()
      val dictionary = dataDesc.rdd.map(r=>(r(0).toString,r(1).toString)).collect().toMap



      rules.collect().foreach { rule =>
        println(
          rule.antecedent.map(s=> dictionary(s)).mkString("[", ",", "]")
            + " => " + rule.consequent.map(s=> dictionary(s)).mkString("[", ",", "]")
            + ", " + rule.confidence)
      }
    }
}