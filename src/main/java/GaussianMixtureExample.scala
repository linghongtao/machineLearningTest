/**
  * Created by root on 7/26/17.
  */
// $example on$
import org.apache.spark.ml.clustering.GaussianMixture
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * An example demonstrating Gaussian Mixture Model (GMM).
  * Run with
  * {{{
  * bin/run-example ml.GaussianMixtureExample
  * }}}
  */
object GaussianMixtureExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
        .master("local")
      .getOrCreate()

    // $example on$
    // Loads data
    val dataset = spark.read.format("libsvm").load("/root/spark-2.1.1/data/mllib/sample_kmeans_data.txt")

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
    val model = gmm.fit(dataset)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }
    // $example off$

    spark.stop()
  }
}
