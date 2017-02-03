package util

import org.apache.spark.sql.SparkSession

trait SparkApp extends App {
  val name: String = this.getClass.getName

  lazy val spark = SparkSession.builder().appName(name).master("local[*]").getOrCreate()
  lazy val sqlContext = spark.sqlContext
}
