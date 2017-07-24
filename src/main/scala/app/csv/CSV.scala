package app.csv

import org.apache.spark.sql.expressions.Window
import util.BaseDriver
import org.apache.spark.sql.functions._

object CSV extends BaseDriver {

  override def run(): Unit = {
    import sqlContext.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jconley/Desktop/window.csv")
    df
      .withColumn("rank", rank.over(Window.partitionBy($"transaction_id").orderBy($"transaction_time".desc, $"error_message")))
      .where("rank > 1")
      .show()
  }
}
