package wordcount

import org.apache.spark.{SparkConf, SparkContext}

object DFSWordCount extends App {

  //local Hadoop install
//  val hadoopUrl = "hdfs://localhost:9001/user/jconley/data/whitman-leaves.txt"

  //Comcast Atlas data center
  val hadoopUrl = "hdfs://nameservice1/user/jconle002c/whitman-leaves.txt"

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Word Count for Walt Whitman poem"))
  val wordsRDD = sc.textFile(hadoopUrl)
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