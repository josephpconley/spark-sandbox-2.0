package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Postgres extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Simple Application"))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._

  val df = sqlContext.read.format("jdbc").options(Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost/dc?user=dc&password=dc",
    "dbtable" -> "owgr_rank"))
  .load()

  //print schema
  df.printSchema()
}