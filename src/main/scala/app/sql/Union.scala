package app.sql

import org.apache.spark.{SparkConf, SparkContext}
import util.BaseDriver

object Union extends BaseDriver {

  override def run(): Unit = {
    val reader = sql.read.format("jdbc").options(Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost/dc?user=dc&password=dc",
    "dbtable" -> "\"user\""))

    val df1 = reader.load()
    val df2 = reader.load()

    df1.union(df2).distinct().show()
  }
}