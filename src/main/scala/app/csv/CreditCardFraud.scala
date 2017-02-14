package csv

import util.BaseDriver

/*
Variables transformed by PCA to preserver anonymity
 */
object CreditCardFraud extends BaseDriver {

  override def run(): Unit = {
    val df = spark.read.option("header", "true").csv("/home/jconley/data/creditcard.csv")
    df.printSchema()
  }
}
