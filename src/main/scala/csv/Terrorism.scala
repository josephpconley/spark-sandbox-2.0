package csv

import org.apache.spark.sql.SparkSession

/*
Variables transformed by PCA to preserver anonymity
 */
object Terrorism extends App {

  val spark = SparkSession.builder().appName("Terrorism").master("local[*]").getOrCreate()
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/jconley/data/terrorism.csv")
  df.printSchema()

  df.where($"country_txt".isNull).where($"iyear".isNull).show()

  val cube = df.cube("country_txt", "iyear")
  cube.count().orderBy($"count".desc).show()
}
