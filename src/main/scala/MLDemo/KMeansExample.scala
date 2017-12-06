package MLDemo

/**
  * Created by root on 11/16/17.
  */
import org.apache.spark.ml.clustering.KMeans
// $example off$
import org.apache.spark.sql.SparkSession
object KMeansExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load("/root/spark-2.1.1/data/mllib/sample_kmeans_data.txt")
    dataset.show(100)
    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()
  }
}
