package MLtest

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/17/17.
  */
object GaussianMixtureTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession
      .builder()
      .appName("GaussianMixtureExampl")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    val DriveTime = sc.read.parquet("/root/sparkdata/DriverTime")

    DriveTime.show(100)


    val TrainData = DriveTime.map(it => (it.getAs("SystemNo").toString,
      Vectors.dense(it.getAs("start_day").toString.toDouble,
        it.getAs("end_day").toString.toDouble,
        it.getAs("delta_Soc").toString.toDouble,
        it.getAs("delta_milige").toString.toDouble,
        it.getAs("delta_time").toString.toDouble))
    ).toDF("label", "features")

    //TrainData.show(100)

    val gmm = new GaussianMixture().setK(3)

    val model = gmm.fit(TrainData)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
        s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }
    // $example off$

    sc.stop()
  }
}
