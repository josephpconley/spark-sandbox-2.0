package app.tab

import util.BaseDriver
import util.Writers

object SEC extends BaseDriver with Writers {

  override def run(): Unit = {
    val df = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", true)
      .csv("/home/jconley/data/2016q4/num.txt")

    df.cache()
    
    println(df.count())
    df.printSchema()

    val tesla = df.where("adsh = '0001564590-16-026820'")
    tesla.show()
    println(tesla.count())

    writeToCSV(tesla, "tesla")
  }
}
