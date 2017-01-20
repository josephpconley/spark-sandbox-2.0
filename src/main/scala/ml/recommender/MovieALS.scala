package ml.recommender

import java.util.Date

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

case class UserRating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

object MovieALS extends App {

  val start = new Date().getTime
  val spark = SparkSession
    .builder()
    .appName("MovieALS")
    .config("spark.executor.memory", "4g")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._
  val sc = spark.sparkContext

  val localDataUrl = "/home/jconley/data/ml-latest-small/ratings.csv"
  val data = sc.textFile(localDataUrl)
  val ratings = data.zipWithIndex().filter(_._2 > 0).map {
    r => r._1.split(",") match {
      case Array(user, item, rate, timestamp) => UserRating(user.toInt, item.toInt, rate.toFloat, timestamp.toLong)
    }
  }.toDF()

  val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

  // Build the recommendation model using ALS on the training data
  val als = new ALS()
    .setMaxIter(5)
    .setRegParam(0.01)
    .setUserCol("userId")
    .setItemCol("movieId")
    .setRatingCol("rating")
  val model = als.fit(training)

  // Evaluate the model by computing the RMSE on the test data
  val predictions = model.transform(test)

  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse")
    .setLabelCol("rating")
    .setPredictionCol("prediction")
  val rmse = evaluator.evaluate(predictions)
  println(s"Root-mean-square error = $rmse")

}
