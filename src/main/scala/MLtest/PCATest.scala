package MLtest

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by root on 11/15/17.
  */
object PCATest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession
      .builder
      .appName("PCAExample")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    val wide = sc.read.parquet("/root/sparkdata/widedata")//label,features

   // wide.show(100)

    val Train = wide.map(it => (it.getAs("SystemNo").toString,
      Vectors.dense(it.getAs("Speed").toString.toDouble,
        it.getAs("VCU_CruisingRange").toString.toDouble,
        it.getAs("BMS_SOC").toString.toDouble,
        it.getAs("IC_Odmeter").toString.toDouble,
        it.getAs("Motor_Temperature").toString.toDouble)
     )).toDF("label", "features")

    val PCAdata: DataFrame = Train.drop("label")

    //PCAdata.show(100)
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(PCAdata)

    val result = pca.transform(PCAdata).select("pcaFeatures")
    result.show(false)


    sc.stop()
  }
}
