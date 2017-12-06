import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.clustering.KMeans
/**
  * Created by root on 7/24/17.
  */
object Hello {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
        .builder()
        .appName(s"${this.getClass.getSimpleName}")
        .master("local")
        .getOrCreate()
    // Loads data.
    val dataset = spark.read.format("libsvm").load("/root/spark-2.1.1/data/mllib/sample_kmeans_data.txt")

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
