package csv

import util.BaseDriver

/*
Variables transformed by PCA to preserver anonymity
 */
object Terrorism extends BaseDriver {

  override def run(): Unit = {
    import sql.implicits._

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jconley/data/terrorism.csv")
    df.printSchema()

    df.where($"country_txt".isNull).where($"iyear".isNull).show()

    val cube = df.cube("country_txt", "iyear")
    cube.count().orderBy($"count".desc).show()
  }
}
