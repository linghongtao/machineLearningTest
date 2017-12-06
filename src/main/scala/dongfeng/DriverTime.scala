package dongfeng

/**
  * Created by root on 10/23/17.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number


object DriverTime {
  def main(args: Array[String]): Unit = {
   /* if(args.length<2){
      """
        |
       """.stripMargin
      sys.exit(0)
    }

    val Array(readpath,basepath)=args*/


    val sc = SparkSession.builder().appName("DriverTime").master("local").getOrCreate()
    //val sc = SparkSession.builder().appName("DriverTime").getOrCreate()
     val DriverStartAndEnd = sc.read.parquet("/root/sparkdata/DriverStartAndEnd").filter("status='drive_end'OR status='drive_start'")
    //val DriverStartAndEnd = sc.read.parquet(readpath).filter("status='drive_end'OR status='drive_start'")

    val DriverStartAndEndRow = DriverStartAndEnd.withColumn("row_num",row_number().over(Window.partitionBy("SystemNo","status").orderBy("CurrentTime")))

    val DriverStart= DriverStartAndEndRow.filter("status='drive_start'")
      .withColumnRenamed("month", "start_month")
      .withColumnRenamed("day", "start_day")
      .withColumnRenamed("hour", "start_hour")
      .withColumnRenamed("minute", "start_minute")
      .withColumnRenamed("Mileage", "start_Mileage")
      .withColumnRenamed("Longitude", "start_Longitude")
      .withColumnRenamed("Latitude", "start_Latitude")
      .withColumnRenamed("CurrentTime", "start_CurrentTime")
      .withColumnRenamed("BMS_SOC", "start_BMS_SOC")


    val DriveEnd = DriverStartAndEndRow.filter("status='drive_end'")
      .withColumnRenamed("month", "end_month")
      .withColumnRenamed("day", "end_day")
      .withColumnRenamed("hour", "end_hour")
      .withColumnRenamed("minute", "end_minute")
      .withColumnRenamed("Mileage", "end_Mileage")
      .withColumnRenamed("Longitude", "end_Longitude")
      .withColumnRenamed("Latitude", "end_Latitude")
      .withColumnRenamed("CurrentTime", "end_CurrentTime")
      .withColumnRenamed("BMS_SOC", "end_BMS_SOC")


    val DriverJoin = DriverStart.join(DriveEnd,Seq("SystemNo","row_num")).drop("row_num").drop("status")

    DriverJoin.createTempView("drv")

    val DriverResult = sc.sql("SELECT SystemNo,start_day,end_day," +
      "(end_BMS_SOC-start_BMS_SOC) AS delta_Soc,(end_Mileage-start_Mileage) AS delta_milige,(((end_day-start_day)*24+(end_hour-start_hour))*60+(end_minute-start_minute))AS delta_time FROM  drv").filter("delta_Soc<0").filter("delta_time>0")


    /*val DriverResult = DriverJoin.select($"SystemNo", $"start_Longitude", $"start_Latitude", $"start_Mileage", $"start_CurrentTime", $"end_Longitude", $"end_Latitude", $"end_Mileage", $"end_CurrentTime",
      ($"end_BMS_SOC" - $"start_BMS_SOC").alias("delta_Soc"), ($"end_Mileage" - $"start_Mileage").alias("delta_milige"),
      ((($"end_day" - $"start_day") * 24 + ($"end_hour" - $"start_hour")) * 60 + ($"end_minute" - $"start_minute")).alias("delta_time")).filter("delta_Soc<0").filter("delta_milige>0").filter("delta_time>0")
*/
    //DriverResult.show(100)
    DriverResult.repartition(1).write.parquet("/root/sparkdata/DriverTime")//"/home/dongdou/sparkdataout/drivertime"

    sc.stop()
  }
}
