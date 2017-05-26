package app.csv

import util.BaseDriver

/*
Variables transformed by PCA to preserver anonymity
 */
object CSV extends BaseDriver {

  override def run(): Unit = {
    import sql.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jconley/Desktop/04122017.csv")
    val dupes = df
      .where("client == 'XRE:X2'")
      .groupBy($"account", $"programmer_id")
      .count()
      .where("count > 1")
      .orderBy($"count".desc)

    dupes.show()
    println(dupes.count())
  }
}
