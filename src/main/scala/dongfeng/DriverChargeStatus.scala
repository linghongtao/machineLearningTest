package dongfeng

/**
  * Created by root on 10/23/17.
  */

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DriverChargeStatus {
  def main(args: Array[String]): Unit = {
    /*if(args.length<2){
      """
        |
       """.stripMargin
      sys.exit(0)
    }

    val Array(readpath,basepath)=args*/

    val sc = SparkSession.builder().appName("DriverChargeStatus").master("local").getOrCreate()
    //val sc = SparkSession.builder().appName("DriverChargeStatus").getOrCreate()
    val widedata = sc.read.parquet("/root/sparkdata/widedata").withColumn("row_num",row_number().over(Window.partitionBy("SystemNo").orderBy("CurrentTime")))
    //val widedata = sc.read.parquet(readpath).withColumn("row_num",row_number().over(Window.partitionBy("SystemNo").orderBy("CurrentTime")))

    widedata.createTempView("wd")

    val widerow1: DataFrame = sc.sql("SELECT  x.SystemNo ,x.Longitude,x.Latitude,x.Speed,x.Direction, x.Elevation,x.Acc,x.IsLocation,x.Mileage,x.CurrentTime, x.BMS_ChargeSt AS xBMS_ChargeSt,y.BMS_ChargeSt AS yBMS_ChargeSt FROM  wd x LEFT JOIN wd y ON x.row_num+1=y.row_num AND x.SystemNo=y.SystemNo ORDER BY  x.row_num")

    val widerow2: DataFrame = sc.sql("SELECT  x.SystemNo ,x.Longitude,x.Latitude,x.Speed,x.Direction, x.Elevation,x.Acc,x.IsLocation,x.Mileage,x.CurrentTime, x.BMS_ChargeSt AS xBMS_ChargeSt,z.BMS_ChargeSt AS zBMS_ChargeSt FROM  wd x LEFT JOIN wd z ON x.row_num-1=z.row_num  AND x.SystemNo=z.SystemNo ORDER BY  x.row_num")

    val widerow = widerow1.join(widerow2,Seq("SystemNo","Longitude","Latitude","Speed","Direction","Elevation","Acc","IsLocation","Mileage","CurrentTime","xBMS_ChargeSt"))

    val widestatus = widerow.withColumn("status", when(widerow("xBMS_ChargeSt") === (1.0) && widerow("zBMS_ChargeSt") === (0.0) && widerow("yBMS_ChargeSt") === (1.0), "charge_start")
      .when(widerow("xBMS_ChargeSt") === (0.0) && widerow("zBMS_ChargeSt") === (0.0) && widerow("yBMS_ChargeSt") === (0.0), "driving")
      .when(widerow("xBMS_ChargeSt") === (1.0) && widerow("zBMS_ChargeSt") === (1.0) && widerow("yBMS_ChargeSt") === (1.0), "charging")
      .when(widerow("xBMS_ChargeSt") === (0.0) && widerow("zBMS_ChargeSt") === (0.0) && widerow("yBMS_ChargeSt") === (1.0), "driver_end")
      .when(widerow("xBMS_ChargeSt") === (0.0) && widerow("zBMS_ChargeSt") === (1.0) && widerow("yBMS_ChargeSt") === (1.0), "drive_start")
      .when(widerow("xBMS_ChargeSt") === (1.0) && widerow("zBMS_ChargeSt") === (1.0) && widerow("yBMS_ChargeSt") === (0.0), "charge_end").otherwise("unknow"))
    //widestatus.show(1000)
    val unknowcount:Double=widestatus.filter("status='unknow'").count()
    val sum :Double= widedata.count()
    println("counts:"+unknowcount/sum)

    widestatus.createTempView("wide")

    //widestatus.show(100)
    val wideday = sc.sql("SELECT SystemNo,Longitude,Latitude,Mileage,month(CurrentTime) as month,day(CurrentTime) as day,status, CurrentTime FROM wide")

    val chargedriverstatus = widedata.join(wideday,Seq("SystemNo","Longitude","Latitude","CurrentTime","Mileage")).drop("row_num")

   // chargedriverstatus.orderBy("SystemNo","CurrentTime").show(1000)
    chargedriverstatus.repartition(1).write.parquet("/root/sparkdata/DriverChargeStatus")

    sc.stop()
  }

}
