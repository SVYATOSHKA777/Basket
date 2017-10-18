import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FPGrowthEx {

  def main(args: Array[String]): Unit = {

    //initialize spark configurations
    val conf = new SparkConf()
    conf.setAppName("market-basket-problem")
    conf.setMaster("local[2]")

    //SparkContext
    val sc = new SparkContext(conf)

    val inputFile = "data/grocery_store_dataset.csv"

    //read file with purchases
    val fileRDD = sc.textFile(inputFile)

    //get transactions
    val products: RDD[Array[String]] = fileRDD.map(s => s.split(","))

    //get frequent patterns via FPGrowth
    val fpg = new FPGrowth()
      .setMinSupport(0.2)

    val model = fpg.run(products)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    //get association rules
    val minConfidence = 0.3
    val rules = model.generateAssociationRules(minConfidence)

    rules.collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }
}