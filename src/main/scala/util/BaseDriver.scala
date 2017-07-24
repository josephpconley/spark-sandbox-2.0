package util

import config.AppConfig
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

trait BaseDriver extends Logging {
  val name: String = this.getClass.getName

  val appConfig = new AppConfig()

  lazy val spark = SparkSession.builder()
    .appName(name)
    .master("local[*]")
    .config("spark.executor.memory", "4g")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/logs/spark-2.1-events")
    .getOrCreate()
  
  lazy val sparkContext = spark.sparkContext
  lazy val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    logger.info("Spark Config")
    spark.conf.getAll.foreach(logger.info)

    val startTime = DateTime.now()
    logger.info(s"Started at ${startTime.toString()}")

    try{
      run()
    } catch {
      case e: Exception =>
        logger.error("ERROR", e)

    } finally {
      val endTime = DateTime.now()
      logger.info(s"Finished at ${endTime.toString()}, took ${(endTime.getMillis.toDouble - startTime.getMillis.toDouble) / 1000} s")

      spark.stop()
    }
  }

  def run(): Unit
}