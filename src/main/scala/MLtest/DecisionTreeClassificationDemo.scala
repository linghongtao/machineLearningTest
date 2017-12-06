package MLtest

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/21/17.
  */
object DecisionTreeClassificationDemo {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("DecisionTreeClassificationDemo").master("local").getOrCreate()

    import sc.implicits._

    val DriverTime = sc.read.parquet("/root/sparkdata/DriverTime")

    val Data = DriverTime.map(it => (it.getAs("SystemNo").toString,
      Vectors.dense(it.getAs("start_day").toString.toDouble,
        it.getAs("end_day").toString.toDouble,
        it.getAs("delta_Soc").toString.toDouble,
        it.getAs("delta_milige").toString.toDouble,
        it.getAs("delta_time").toString.toDouble))
    ).toDF("label", "features")

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(Data)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(6)
      .fit(Data)

    val Array(trainingData, testData) = Data.randomSplit(Array(0.7,0.3))


    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")


    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)



    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")


    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))


    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)
    // $example off$

    sc.stop()
  }
}
