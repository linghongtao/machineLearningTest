package SparkCoreTest

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by root on 11/22/17.
  */
object DataFrameUion {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("DataFrameUion").master("local").getOrCreate()

    import sc.implicits._

    val df = sc.sparkContext.parallelize(Seq(
      (1441637160, 10.0),
      (1441637170, 20.0),
      (1441637180, 30.0),
      (1441637210, 40.0),
      (1441637220, 10.0),
      (1441637230, 0.0))).toDF("timestamp", "value")


    val newdf = sc.sparkContext.parallelize(Seq(
      (1441637161, 11.0),
      (1441637171, 21.0),
      (1441637181, 31.0),
      (1441637211, 41.0),
      (1441637221, 11.0),
      (1441637231, 1.0))).toDF("timestamp", "value")

    val resultdf = df.union(newdf)


    resultdf.show(100)


    sc.stop()
  }
}
