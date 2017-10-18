import org.apache.spark.{SparkConf, SparkContext}

object Numbers {
  def main(args: Array[String]): Unit = {
    val inputFile = "numbers.txt"
    val conf = new SparkConf().setAppName("wordCount")
    conf.setMaster("local[2]")


    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)
    val words = input.map(line => line.split(" "))
    val numbersArray = words.map(s => s.map(a => a.toInt))
    val scc = numbersArray.map(d => d.reduce((a, b) => a + b))
    val filtered = numbersArray.map(s => s.filter(a => a % 5 == 0))
    val ans = filtered.map(d => d.sum)
    scc.foreach(println)
    ans.foreach(println)
  }
}
