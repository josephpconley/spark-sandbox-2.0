package ml.recommender

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by jconley on 9/30/16.
  */
object MovieLensDataInspector extends App {
  val session = SparkSession.builder().master("local").appName("Movie Recommender").getOrCreate()
  val sc = session.sparkContext
  val localDataBaseUrl = "/home/jconley/data/ml-latest"

  // Load and parse the data downloaded from http://grouplens.org/datasets/movielens/
  val localRatings = s"$localDataBaseUrl/ratings.csv"
  val data: RDD[String] = sc.textFile(localRatings)
  val ratings = data.zipWithIndex().filter(_._2 > 0).map { r => r._1.split(",") match {
    case Array(user, item, rate, timestamp) => Rating(user.toInt, item.toInt, rate.toDouble)
  }}

//  val ds = session.createDataset(ratings)

  //find out which movies have been reviewed the most
//  ds.show(10)


  //join against the movies.csv to get title info
}
