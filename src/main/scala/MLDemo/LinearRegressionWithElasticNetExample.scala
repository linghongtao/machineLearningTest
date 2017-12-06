package MLDemo

/**
  * Created by root on 11/15/17.
  */
import org.apache.spark.ml.regression.LinearRegression
// $example off$
import org.apache.spark.sql.SparkSession
object LinearRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("LinearRegressionWithElasticNetExample")
      .master("local")
      .getOrCreate()

    // $example on$
    // Load training data
    val training = spark.read.format("libsvm")
      .load("/root/spark-2.1.1/data/mllib/sample_linear_regression_data.txt")

    training.select("features").show(100)


    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    // $example off$

    spark.stop()
  }
}
