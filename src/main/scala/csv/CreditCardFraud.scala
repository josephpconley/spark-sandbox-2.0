package csv

import util.SparkApp

/*
Variables transformed by PCA to preserver anonymity
 */
object CreditCardFraud extends SparkApp {

  val df = spark.read.option("header", "true").csv("/home/jconley/data/creditcard.csv")
  df.printSchema()
}
