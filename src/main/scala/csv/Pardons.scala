package csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Pardons extends App {

  val spark = SparkSession.builder().appName("Pardons").master("local[*]").getOrCreate()
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  val df = spark.read.option("header", "true").csv("/home/jconley/data/pardons/*.csv")
  df.printSchema()

  val sums = df.groupBy("President").agg(sum("pardons").as("total_pardons"), sum("commutations").as("total_commutations")).orderBy($"pardons".desc)
  sums.show()
}
