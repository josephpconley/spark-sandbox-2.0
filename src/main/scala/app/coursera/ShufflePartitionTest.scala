package app.coursera

import util.{BaseDriver, RandomData, Timing, Writers}

case class Purchase(customerId: String, price: Double)

/**
  * From Week 3 of Big Data Anaylsis with Spark
  *
  * On cluster:
  * groupByKey - 15.48s
  * reduceBykey - 4.65s
  * reduceByKey w/ range partitioning - 1.79s
  *
  * Locally
  * groupByKey - 279ms
  * reduceBykey - 178ms
  * reduceByKey w/ range partitioning - ???

  *
  * TODO test on cluster!
  */
object ShufflePartitionTest extends BaseDriver with Writers with RandomData with Timing {

  override def run(): Unit = {
    val customerIds = Seq("001", "002", "003", "004", "005")
    val purchases = sparkContext.parallelize( (0 to 100000).map(_ => Purchase(generateFromSeq(customerIds), generateDouble(5.0, 10.0))))
    purchases.cache()
    
    
    timed("grouped", purchases
      .map(p => p.customerId -> p.price)
      .groupByKey()
      .count())

    timed("reduced", purchases
      .map(p => p.customerId -> (1, p.price))
      .reduceByKey( (p1, p2) => (p1._1 + p2._1, p1._2 + p2._2))
      .count())
  }
}
