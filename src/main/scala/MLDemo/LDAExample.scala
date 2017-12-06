package MLDemo

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/21/17.
  */
object LDAExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("/root/spark-2.1.1/data/mllib/sample_lda_libsvm_data.txt")

    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    println(s"The los:wer bound on the log likelihood of the entire corpu $ll")
    println(s"The upper bound bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)

    transformed.show(false)
    // $example off$

    spark.stop()
  }
}
