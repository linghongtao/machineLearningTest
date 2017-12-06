package MLDemo
// $example on$
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
// $example off$
import org.apache.spark.sql.SparkSession
/**
  * Created by root on 11/16/17.
  */
object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("OneHotEncoderExample")
      .master("local")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    df.show()
    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)

    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.show()
    // $example off$

    spark.stop()
  }
}
