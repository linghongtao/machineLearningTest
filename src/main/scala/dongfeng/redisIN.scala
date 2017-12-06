package dongfeng

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

/**
  * Created by root on 10/24/17.
  */

object redisIN {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("redisIN").master("local").getOrCreate()

    val widedata = sc.read.parquet("/root/sparkdata/widedata")

    val widedata_rownum = widedata.withColumn("roe_num",row_number().over(Window.partitionBy("SystemNo").orderBy("CurrentTime")))

    widedata_rownum.show(100)

    sc.stop()
  }
}
