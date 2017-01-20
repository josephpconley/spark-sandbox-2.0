package ml.recommender

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by jconley on 9/30/16.
  */
object MovieRecommender extends App {
  val session = SparkSession.builder().master("local").appName("Movie Recommender").getOrCreate()
  val sc = session.sparkContext
  val sql = session.sqlContext

  // Load and parse the data downloaded from http://grouplens.org/datasets/movielens/
  val localDataUrl = "/home/jconley/data/ml-latest-small/ratings.csv"
  val data: RDD[String] = sc.textFile(localDataUrl)
  val ratings = data.zipWithIndex().filter(_._2 > 0).map { r => r._1.split(",") match {
    case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)
  }}

  sql.createDataFrame(ratings).show(100)

  // Build the recommendation model using ALS
  val rank = 10
  val numIterations = 10
  val model = ALS.train(ratings, rank, numIterations, 0.01)

  // Evaluate the model on rating data
  //TODO get separate training and test data, this uses same dataset, that's bad umm k!
  val usersProducts = ratings.map { case Rating(user, product, rate) =>
    (user, product)
  }

  val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }

  val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }.join(predictions)

  val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
    val err = r1 - r2
    err * err
  }.mean()

  println("Mean Squared Error = " + MSE)

  // Save and load model
//  model.save(sc, "target/tmp/myCollaborativeFilter")
//  val sameModel = MatrixFactorizationModel.load(sc, "target/tmp/myCollaborativeFilter")

  //Implicit data source (transactions, behavior)
//  val alpha = 0.01
//  val lambda = 0.01
//  val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
}
