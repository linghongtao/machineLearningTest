package MLtest

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/16/17.
  */
object KmeansTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("KmeansTest").master("local").getOrCreate()
    import sc.implicits._

    val wide = sc.read.parquet("/root/sparkdata/widedata")

    wide.show(100)

    val Train = wide.map(it => (it.getAs("SystemNo").toString,
      Vectors.dense(it.getAs("Speed").toString.toDouble,
        it.getAs("VCU_CruisingRange").toString.toDouble,
        it.getAs("BMS_SOC").toString.toDouble,
        it.getAs("IC_Odmeter").toString.toDouble,
        it.getAs("Motor_Temperature").toString.toDouble,
        it.getAs("Motor_Revolution").toString.toDouble)
    )).toDF("label", "features")

    val kmeans = new KMeans().setK(7).setSeed(1L)
    val model = kmeans.fit(Train)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(Train)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$


    sc.stop()
  }
}
