package MLtest

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/16/17.
  */
object OneHotEncoderTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .appName("OneHotEncoderTest")
      .master("local")
      .getOrCreate()

    import sc.implicits._

    val charge = sc.read.parquet("/root/sparkdata/DriverChargeStatus")

    charge.show(100)

    val indexer = new StringIndexer()
      .setInputCol("status")
      .setOutputCol("statusIndex")
      .fit(charge)

    val indexed = indexer.transform(charge)

    val encoder = new OneHotEncoder()
      .setInputCol("statusIndex")
      .setOutputCol("statusVec")

    val encoded = encoder.transform(indexed)
    encoded.show()

    sc.stop()
  }
}
