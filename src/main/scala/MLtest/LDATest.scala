package MLtest

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/21/17.
  */
object LDATest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("object LDATest").master("local").getOrCreate()
    import sc.implicits._

    val DriverTime = sc.read.parquet("/root/sparkdata/DriverTime")
    sc.stop()
  }
}
