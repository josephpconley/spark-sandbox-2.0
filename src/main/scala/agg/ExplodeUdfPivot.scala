package agg

import util.SparkApp

case class Sales(id: Int, account: String, year: String, commission: Int, sales_reps: Seq[String])

/**
  * From Data Science in Scala on Big Data University
  * Module 3 - Explode, UDFs, and Pivot
  *
  * https://courses.bigdatauniversity.com/courses/course-v1:BigDataUniversity+SC0105EN+2016/courseware/366f34a7b4944bd68b946ae3da88270c/54938b466e384d48b87b4e4da793c41e/
  */
object ExplodeUdfPivot extends SparkApp {

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val sales = Seq(
    Sales(1, "Acme", "2013", 1000, Seq("Jim", "Tom")),
    Sales(2, "Lumos", "2013", 1100, Seq("Fred", "Ann")),
    Sales(3, "Acme", "2014", 2800, Seq("Jim")),
    Sales(4, "Lumos", "2014", 1200, Seq("Ann")),
    Sales(5, "Acme", "2014", 4200, Seq("Fred", "Sally"))
  ).toDF()

  /**
    * we want to
    * - use .explode() to generate one row per sales person
    * - commission is split evenly so we need a way to factor that
    * - group by each sales person and get yearly totals
    */

  val columnLen = udf { sales: Seq[String] =>
    sales.length
  }
    
  val exploded = sales.select(
    $"id",
    $"account",
    $"year",
    $"commission",
    ($"commission" / columnLen($"sales_reps")).as("share"),
    explode($"sales_reps").as("sales_rep"))

  exploded.show()

  //use pivot to see annual totals for each salesperson
  exploded
    .groupBy($"sales_rep")
    .pivot("year")
    .sum("share")
    .orderBy("sales_rep")
    .show()

  
}