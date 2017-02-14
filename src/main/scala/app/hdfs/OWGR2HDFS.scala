package hdfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object OWGR2HDFS extends App {

  val spark = SparkSession
    .builder()
    .appName("OWGR app")
    .master("local[4]")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  // this is used to implicitly convert an RDD to a DataFrame.

  val df = sqlContext.read.format("jdbc").options(Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost/dc?user=dc&password=dc",
    "dbtable" -> "owgr_rank"))
  .load()

  //print schema
  df.printSchema()

  //show me the top 20 OWGR
  val ds = df.where("this_week < 20").sort("week", "this_week")
  ds.show(100)
  ds.write.format("json").save("hdfs://localhost:9001/user/jconley/data/owgr.json")
}