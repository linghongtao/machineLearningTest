package MLtest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by root on 11/21/17.
  */
object EDATest {
  def main(args: Array[String]): Unit = {
    if(args.length<3){
      """
        |
       """.stripMargin
      sys.exit(0)
    }
    val Array(readpath,colstr,writepath)=args

    // val sc = SparkSession.builder().appName("ColnumStatisticDemo").master("local").getOrCreate()

    val sc = SparkSession.builder().appName("ColnumStatisticDemo").getOrCreate()
    import sc.implicits._


    val widedata = sc.read.parquet(readpath)

    //widedata.show(100)

    //val colstr = "Mileage"
    val colnum = widedata.select(colstr)

    colnum.createTempView("colnum")
    //colnum.groupBy("").min()

    //max.show(10)


    val stats: DataFrame = sc.sql("SELECT max("+colstr+") as max,min("+colstr+") AS min ,avg("+colstr+") AS avg ,stddev("+colstr+") AS stddev,variance("+colstr+") AS variance, " +
      "skewness("+colstr+") AS skewness,kurtosis("+colstr+") AS kurtosis FROM colnum ")




    val statsWithColName = stats.withColumn("ColnumName",stats("max"))//

    val StatsResultWithColName = statsWithColName.withColumn("ColnumName",when( statsWithColName("ColnumName")=== statsWithColName("max"),colstr))


    //StatsResultWithColName .show(100)

    StatsResultWithColName.write.parquet(writepath)
    sc.stop()
  }
}
