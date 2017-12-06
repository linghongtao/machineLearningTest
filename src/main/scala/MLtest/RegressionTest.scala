package MLtest

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/15/17.
  */
object RegressionTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("RegressionTest").master("local").getOrCreate()

    import sc.implicits._

    val wide = sc.read.parquet("/root/sparkdata/widedata")//label,features

    //wide.show(100)

    val Train = wide.map(it => (it.getAs("BMS_SOC").toString.toDouble,
      Vectors.dense(it.getAs("Speed").toString.toDouble,
        it.getAs("VCU_CruisingRange").toString.toDouble))).toDF("label", "features")

    Train.show(100)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    val llrModel = lr.fit(Train)

    sc.stop()
  }
}
