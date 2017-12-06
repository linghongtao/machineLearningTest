package dongfeng

/**
  * Created by root on 10/23/17.
  */

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object RecycleCount {
  def main(args: Array[String]): Unit = {
    /*if (args.length < 2) {
      """
        |
       """.stripMargin
      sys.exit(0)
    }

    val Array(readpath, basepath) = args*/

    val sc = SparkSession.builder().appName("RecycleCount").master("local").getOrCreate()
    //val sc = SparkSession.builder().appName("RecycleCount").getOrCreate()

     val DriverIn: DataFrame = sc.read.parquet("/root/sparkdata/DriverStartAndEnd")
    //val DriverIn: DataFrame = sc.read.parquet(readpath)


      val DriverStartAndEnd=DriverIn.drop("VCU_CruisingRange").drop("Motor_Temperature").drop("BMS_ChargeFS").drop("IC_Odmeter")
      .filter("status='drive'OR status='drive_end'OR status='drive_start'").drop("Direction").drop("Elevation").drop("Acc").drop("IsLocation").drop("Motor_Revolution").drop("BMS_ChargeSt")

    val Recycle = DriverStartAndEnd.withColumn("Recy",
      when(DriverStartAndEnd("status") === ("drive") && DriverStartAndEnd("Motor_OutputPower") >= 0, "driving")
        .when(DriverStartAndEnd("status") === ("drive") && DriverStartAndEnd("Motor_OutputPower") < 0, "recycle")
        .when(DriverStartAndEnd("status") === ("drive_start"), "drive_start")
        .when(DriverStartAndEnd("status") === ("drive_end"), "drive_end")).drop("status")

    val RecycleRow = Recycle.withColumn("row_num", row_number().over(Window.partitionBy("SystemNo").orderBy("CurrentTime"))).drop("Speed").drop("Motor_OutputPower")

    RecycleRow.createTempView("recy")

    val recy1: DataFrame = sc.sql("SELECT  x.SystemNo ,x.Longitude,x.Latitude,x.Mileage,x.CurrentTime, x.BMS_SOC,x.month,x.day,x.hour,x.minute,x.Recy AS xRecy,y.Recy AS yRecy FROM  recy x LEFT JOIN recy y ON x.row_num+1=y.row_num AND x.SystemNo=y.SystemNo ORDER BY  x.row_num")

    val recy2: DataFrame = sc.sql("SELECT  x.SystemNo ,x.Longitude,x.Latitude,x.Mileage,x.CurrentTime, x.BMS_SOC,x.month,x.day,x.hour,x.minute,x.Recy AS xRecy,z.Recy AS zRecy FROM  recy x LEFT JOIN recy z ON x.row_num-1=z.row_num AND x.SystemNo=z.SystemNo ORDER BY  x.row_num")


    val recyxy = recy1.join(recy2, Seq("SystemNo", "Longitude", "Latitude", "CurrentTime", "Mileage", "month", "day", "hour", "minute", "BMS_SOC", "xRecy"))

    val RecyStartAndEnd = recyxy.withColumn("recy_status", when(recyxy("xRecy") === ("driving") && recyxy("yRecy") === ("recycle") && recyxy("zRecy") === ("driving"), "recycle_start")
      .when(recyxy("xRecy") === ("driving") && recyxy("yRecy") === ("driving") && recyxy("zRecy") === ("recycle"), "recycle_end")
      .when(recyxy("xRecy") === ("driving") && recyxy("yRecy") === ("drive_end") && recyxy("zRecy") === ("recycle"), "recycle_end")
      .when(recyxy("xRecy") === ("drive_end") && recyxy("yRecy") === ("drive_start") && recyxy("zRecy") === ("recycle"), "recycle_end"))
      .filter("recy_status='recycle_start'OR recy_status='recycle_end'").drop("xRecy").drop("yRecy").drop("zRecy")


    val RecyStartAndEndRow = RecyStartAndEnd.withColumn("row_num", row_number().over(Window.partitionBy("SystemNo", "recy_status").orderBy("CurrentTime")))

    val RecyStart = RecyStartAndEndRow.filter("recy_status='recycle_start'")
      .withColumnRenamed("month", "start_month")
      .withColumnRenamed("day", "start_day")
      .withColumnRenamed("hour", "start_hour")
      .withColumnRenamed("minute", "start_minute")
      .withColumnRenamed("Mileage", "start_Mileage")
      .withColumnRenamed("Longitude", "start_Longitude")
      .withColumnRenamed("Latitude", "start_Latitude")
      .withColumnRenamed("CurrentTime", "start_CurrentTime")
      .withColumnRenamed("BMS_SOC", "start_BMS_SOC")


    val RecyEnd = RecyStartAndEndRow.filter("recy_status='recycle_end'")
      .withColumnRenamed("month", "end_month")
      .withColumnRenamed("day", "end_day")
      .withColumnRenamed("hour", "end_hour")
      .withColumnRenamed("minute", "end_minute")
      .withColumnRenamed("Mileage", "end_Mileage")
      .withColumnRenamed("Longitude", "end_Longitude")
      .withColumnRenamed("Latitude", "end_Latitude")
      .withColumnRenamed("CurrentTime", "end_CurrentTime")
      .withColumnRenamed("BMS_SOC", "end_BMS_SOC")

    val RecyJoin = RecyEnd.join(RecyStart, Seq("SystemNo", "row_num")).drop("row_num").drop("recy_status")

    RecyJoin.createTempView("rec")

    val RecyResult = sc.sql("SELECT SystemNo,start_Longitude,start_Latitude,start_Mileage,start_CurrentTime,end_Longitude,end_Latitude,end_Mileage ,end_CurrentTime ,start_day,end_day," +
      "(end_BMS_SOC-start_BMS_SOC) AS delta_Soc,(end_Mileage-start_Mileage) AS delta_milige,(((end_day-start_day)*24+(end_hour-start_hour))*60+(end_minute-start_minute))AS delta_time FROM  rec").filter("delta_time>0")



    //RecyResult.repartition(1).write.parquet(basepath)
    sc.stop()
  }
}
