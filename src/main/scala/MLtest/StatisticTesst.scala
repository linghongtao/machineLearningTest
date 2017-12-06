package MLtest

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/8/17.
  */
object StatisticTesst {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("StatisticTesst").master("local").getOrCreate()

    sc.read.parquet("/root/sparkdata/widedata")
    sc.stop()
  }
}
