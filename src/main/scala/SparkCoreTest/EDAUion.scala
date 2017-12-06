package SparkCoreTest

import org.apache.hadoop.util.hash.Hash
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * Created by root on 11/22/17.
  */
object EDAUion {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("EDAUion").master("local").getOrCreate()

    import sc.implicits._

    val widedata = sc.read.parquet("/root/sparkdata/widedata")

    val schema: StructType = widedata.schema

   val schemaMap = new mutable.HashMap[String,String]()

    //schema.map(item=>println(item.name+"  "+item.dataType))

    schema.map(item=>{
      schemaMap.put(item.name.toString,item.dataType.toString)
    })

    schemaMap.map(item=>println(item._1,item._2))


    var newdf = sc.sparkContext.parallelize(Seq(
      (0.0, 0.0,0.0,0.0,0.0,0.0,0.0,"na"))).toDF("max","min","avg","stddev","variance","skewness","kurtosis","ColnumName")


    schemaMap.map(item=>{
      if(item._2.toString.equals("DoubleType")||item._2.toString.equals("StringType")){
        val colstr = item._1.toString
        val colnum = widedata.select(colstr)

        colnum.createTempView(colstr+"table")
        //colnum.groupBy("").min()

        //max.show(10)


        val stats: DataFrame = sc.sql("SELECT max("+colstr+") as max,min("+colstr+") AS min ,avg("+colstr+") AS avg ,stddev("+colstr+") AS stddev,variance("+colstr+") AS variance, " +
          "skewness("+colstr+") AS skewness,kurtosis("+colstr+") AS kurtosis FROM "+colstr+"table")




        val statsWithColName = stats.withColumn("ColnumName",stats("max"))//

        val StatsResultWithColName = statsWithColName.withColumn("ColnumName",when( statsWithColName("ColnumName")=== statsWithColName("max"),colstr))

        newdf=newdf.union(StatsResultWithColName)
      }
    })


    newdf.show(100)
    sc.stop()
  }
}
