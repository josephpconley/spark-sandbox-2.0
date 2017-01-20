package csv

import org.apache.spark.sql.SparkSession

/*
Variables transformed by PCA to preserver anonymity
 */
object CreditCardFraud extends App {

  val spark = SparkSession.builder().appName("CreditCardFraud").master("local[*]").getOrCreate()
  val sqlContext = spark.sqlContext

  val df = spark.read.option("header", "true").csv("/home/jconley/data/creditcard.csv")
  df.printSchema()
}
