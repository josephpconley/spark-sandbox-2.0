package sql

import org.apache.spark.{SparkConf, SparkContext}

object MySQL extends App {

  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Simple Application"))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  // this is used to implicitly convert an RDD to a DataFrame.
  import sqlContext.implicits._


  val jdbc = sqlContext.read.format("jdbc").options(Map(
    "driver" -> "com.mysql.jdbc.Driver",
    "url" -> "jdbc:mysql://localhost:3307/test?user=root&password=arvydas11",
    "dbtable" -> "aminno_member_email"))
  .load()

  println(jdbc.count())
}