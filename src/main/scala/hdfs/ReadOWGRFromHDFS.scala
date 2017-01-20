package hdfs

import org.apache.spark.sql.SparkSession

object ReadOWGRFromHDFS extends App {

  val ss = SparkSession.builder().master("local").appName("Spark SQL Example").getOrCreate()
  val json = ss.read.json("hdfs://localhost:9001/user/jconley/data/owgr.json")

  json.select("week", "this_week", "name")
    .sort("week", "this_week")
    .show(100)
}