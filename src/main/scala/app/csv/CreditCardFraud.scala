package csv

import org.apache.spark.sql.Column
import util.BaseDriver

/*
Variables transformed by PCA to preserver anonymity
 */
object CreditCardFraud extends BaseDriver {

  override def run(): Unit = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jconley/data/creditcard.csv")
    df.printSchema()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val variables: List[Column] = (1 to 28).map(i => column("V" + i)).toList
    val proj = variables.reduce((c1, c2) => c1 + c2).as("sum")

    df.select(col("time"), proj)
      .show(20)

  }
}