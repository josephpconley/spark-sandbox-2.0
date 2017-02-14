package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Simple Application"))
  val wordsRDD = sc.textFile("whitman-leaves.txt")
    .flatMap(_.trim.split("""[\s\W]+"""))
    .filter(_.nonEmpty)
    .map(word => (word.toLowerCase, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .take(10)
    .foreach { case (word, count) =>
    println(word + " " + count)
  }
}