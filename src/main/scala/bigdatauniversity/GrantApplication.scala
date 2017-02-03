package bigdatauniversity

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import util.SparkApp

/**
  * Predicting Grant Applications
  *
  * Module 5 - Data Science with Scala
  *
  * https://www.kaggle.com/c/unimelb
  * https://www.kaggle.com/c/unimelb/data
  *
  * https://s3.eu-central-1.amazonaws.com/dsr-data/grants/grantsPeople.csv
  *
  * A2, A, B, C - journals ranked by prestige in descending order
  *
  * Ideas for features?
  * - With_PHD
  * - No_of_years
  * - Number_of_successful_grant/unsuccessful
  * - Grant_Category
  * - Grant_Value
  *
  */
object GrantApplication extends SparkApp {

  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val basePath = "src/main/scala/bigdatauniversity"
  val grantData = spark.read
    .options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> "\t"))
    .csv(s"$basePath/grantsPeople.csv")
    .cache()

  //more convenient to work with 0/1 integer
  //weight A2/A journals heavier
  val researchers = grantData
    .withColumn("phd",
      when(col("With_PHD").equalTo("Yes"), 1).otherwise(0))
    .withColumn("CI", col("Role").equalTo("CHIEF_INVESTIGATOR").cast("Int"))
    .withColumn("paperscore", col("A2") * 4 + col("A") * 3)

  val grants = researchers.groupBy("Grant_Application_ID").agg(
    max("Grant_Status").as("Grant_Status"),
    max("Grant_Category_Code").as("Category_Code"),
    max("Contract_Value_Band").as("Value_Band"),
    sum("phd").as("PHDs"),
    when(max(expr("paperscore * CI")).isNull, 0)
      .otherwise(max(expr("paperscore * CI"))).as("paperscore"),
    count("*").as("teamsize"),
    when(sum("Number_of_Successful_Grant").isNull, 0)
      .otherwise(sum("Number_of_Successful_Grant")).as("successes"),
    when(sum("Number_of_Unsuccessful_Grant").isNull, 0)
      .otherwise(sum("Number_of_Unsuccessful_Grant")).as("failures")
  )

  grants.show(5)

  val training = grants.filter("Grant_Application_ID < 6635")
  val test = grants.filter("Grant_Application_ID >= 6635")
  println("Training count " + training.count)
  println("Test count " + test.count)

  //use StringIndexer to convert categorical columns into ints so we can work with the Spark libraries
  val valueBandIndexer = new StringIndexer().setInputCol("Value_Band").setOutputCol("Value_index").fit(grants)
  val categoryIndexer = new StringIndexer().setInputCol("Category_Code").setOutputCol("Category_index").fit(grants)
  val labelIndexer = new StringIndexer().setInputCol("Grant_Status").setOutputCol("status").fit(grants)

  //gather features into vector
  val assembler = new VectorAssembler().setInputCols(Array(
    "Value_index", "Category_index", "PHDs", "paperscore", "teamsize", "successes", "failures"
  )).setOutputCol("assembled")

  //build classifier, set seed to reproduce results
  val rf = new RandomForestClassifier().setFeaturesCol("assembled").setLabelCol("status").setSeed(42)

  val pipeline = new Pipeline().setStages(Array(valueBandIndexer, categoryIndexer, labelIndexer, assembler, rf))

  //evaluator, default metric for classifier is area under curve
  val areaUnderCurveEval = new BinaryClassificationEvaluator()
    .setLabelCol("status")
    .setRawPredictionCol("prediction")

  //TODO pipeline concept to JUST do transforms?


  //original results w/o  ut tuning 0.8286825031477815
//  val model = pipeline.fit(training)
//  val test_results = model.transform(test)
//  println("AOC: " + areaUnderCurveEval.evaluate(test_results))

  /**
    * Tuning
    */

  println(rf.extractParamMap())

  /**
    * above map shows that default maxDepth is 5 and default maxDepth is 20, maybe we can do better
    * we'll try a grid of parameters (all combinations of below values) to find the best params
    */

  val paramGrid = new ParamGridBuilder()
    .addGrid(rf.maxDepth, Array(10, 30))
    .addGrid(rf.numTrees, Array(10, 100))
    .build()

  /**
    * TODO grok k-fold cross validation
    *
    */
  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(areaUnderCurveEval)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(3)

  //with tuning
  val cvModel = cv.fit(training)
  val cvResults = cvModel.transform(test)
  println("AOC with tuning: " + areaUnderCurveEval.evaluate(cvResults))

  //which model was best?
//  bestEstimatorParammap

// val bestModel = cvModel.bestModel.asInstanceOf[Pipeline]
//  bestModel.featureImportances to evaluate features

  //TODO run tuning on a cluster to improve speeds!  Measuring timing, maybe setup Home cluster with 3 machines?

  spark.stop()
}

trait BestParamMap {

  def bestEstimatorParamMap(cvModel: CrossValidatorModel): (ParamMap, Double) = {
    cvModel.getEstimatorParamMaps
      .zip(cvModel.avgMetrics)
      .maxBy(_._2)
  }

}
