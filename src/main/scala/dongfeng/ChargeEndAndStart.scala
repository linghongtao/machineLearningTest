package dongfeng

/**
  * Created by root on 10/23/17.
  */

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{hour, minute, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ChargeEndAndStart {
  def main(args: Array[String]): Unit = {
   /* if(args.length<2){
      """
        |
       """.stripMargin
      sys.exit(0)
    }

    val Array(readpath,basepath)=args*/

    val sc = SparkSession.builder().appName("ChargeEndAndStart").master("local").getOrCreate()
    //val sc = SparkSession.builder().appName("ChargeEndAndStart").getOrCreate()
    val ChargeStartAndEnd = sc.read.parquet("/root/sparkdata/DriverChargeStatus").filter("status='charge_start'OR status='charge_end'").drop("Speed").drop("Direction").drop("Elevation").drop("Acc").drop("IsLocation").drop("row_num").drop("Motor_Revolution").drop("IC_Odmeter")
    //val ChargeStartAndEnd = sc.read.parquet(readpath).filter("status='charge_start'OR status='charge_end'").drop("Speed").drop("Direction").drop("Elevation").drop("Acc").drop("IsLocation").drop("row_num").drop("Motor_Revolution").drop("IC_Odmeter")


    val ChargeStartAndEndHM = ChargeStartAndEnd.withColumn("hour",hour(ChargeStartAndEnd("CurrentTime"))).withColumn("minute",minute(ChargeStartAndEnd("CurrentTime"))).drop("BMS_ChargeSt").drop("Motor_OutputPower").drop("BMS_ChargeFS").drop("VCU_CruisingRange").drop("Motor_Temperature")

    val ChargeStartAndEndHMRow: DataFrame = ChargeStartAndEndHM.withColumn("row_num",row_number().over(Window.partitionBy("SystemNo","status").orderBy("CurrentTime")))

    val ChargeStart = ChargeStartAndEndHMRow.filter("status='charge_start'")
      .withColumnRenamed("month","start_month")
      .withColumnRenamed("day","start_day")
      .withColumnRenamed("hour","start_hour")
      .withColumnRenamed("minute","start_minute")
      .withColumnRenamed("Mileage","start_Mileage")
      .withColumnRenamed("Longitude","start_Longitude")
      .withColumnRenamed("Latitude","start_Latitude")
      .withColumnRenamed("CurrentTime","start_CurrentTime")
      .withColumnRenamed("BMS_SOC","start_BMS_SOC")


    val ChargeEnd = ChargeStartAndEndHMRow.filter("status='charge_end'")
      .withColumnRenamed("month","end_month")
      .withColumnRenamed("day","end_day")
      .withColumnRenamed("hour","end_hour")
      .withColumnRenamed("minute","end_minute")
      .withColumnRenamed("Mileage","end_Mileage")
      .withColumnRenamed("Longitude","end_Longitude")
      .withColumnRenamed("Latitude","end_Latitude")
      .withColumnRenamed("CurrentTime","end_CurrentTime")
      .withColumnRenamed("BMS_SOC","end_BMS_SOC")



    val Charge_Start_End = ChargeStart.join(ChargeEnd,Seq("SystemNo","row_num")).drop("row_num").drop("status")
    Charge_Start_End.createTempView("charge")

    val ChargeEndAndStartTime: DataFrame = sc.sql("SELECT SystemNo," +
      "(end_BMS_SOC-start_BMS_SOC) AS delta_Soc,(((end_day-start_day)*24+(end_hour-start_hour))*60+(end_minute-start_minute))AS delta_time FROM  charge").filter("delta_Soc>0").filter("delta_time>0").filter("delta_time<735")


    /*val ChargeEndAndStartTime = Charge_Start_End.select($"SystemNo", $"start_Longitude", $"start_Latitude", $"start_Mileage", $"start_CurrentTime", $"end_Longitude", "end_Latitude", "end_Mileage", "end_CurrentTime",
      ("end_BMS_SOC"-"start_BMS_SOC").alias("delta_Soc"), ("end_Mileage"-"start_Mileage").alias("delta_milige"),
      ((("end_day"-"start_day") * 24 + ("end_hour"-"start_hour")) * 60 + ("end_minute"-"start_minute")).alias("delta_time")).filter("delta_Soc>0").filter("delta_time>0").drop("delta_milige")*/



   // ChargeEndAndStartTime.repartition(1).write.parquet(basepath)
    ChargeEndAndStartTime.show(1000)

    sc.stop()
  }
}
