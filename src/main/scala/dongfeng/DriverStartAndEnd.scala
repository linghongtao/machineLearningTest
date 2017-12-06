package dongfeng

/**
  * Created by root on 10/23/17.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{hour, minute, row_number, when}


object DriverStartAndEnd {
  def main(args: Array[String]): Unit = {
   /* if(args.length<2){
      """
        |
       """.stripMargin
      sys.exit(0)
    }

    val Array(readpath,basepath)=args*/


    val sc = SparkSession.builder().appName("DriverStartAndEnd").master("local").getOrCreate()
   // val sc = SparkSession.builder().appName("DriverStartAndEnd").getOrCreate()
   val driver = sc.read.parquet("/root/sparkdata/DriverChargeStatus").drop("row_num").filter("status!='charging'").filter("status!='charge_end'").filter("status!='charge_start'").drop("status")
    //val driver = sc.read.parquet(readpath).drop("row_num").filter("status!='charging'").filter("status!='charge_end'").filter("status!='charge_start'").drop("status")

    val driver_row = driver.withColumn("row_num",row_number().over(Window.partitionBy("SystemNo").orderBy("CurrentTime")))
    driver_row.createTempView("driver")
    val driverX = sc.sql("SELECT  X.SystemNo,X.Longitude,X.Latitude,X.Speed,X.CurrentTime,Y.Longitude-X.Longitude AS del_Longitude,Y.Latitude-X.Latitude as del_Latitude,Y.Mileage-X.Mileage as del_Mileage FROM driver X LEFT JOIN driver Y ON X.SystemNo=Y.SystemNo AND X.row_num+1=Y.row_num ORDER BY X.row_num")


    val stop = driverX.withColumn("stop",when(driverX("del_Longitude")===(0.0)&&driverX("del_Latitude")===(0.0)&&driverX("del_Mileage")===(0.0)&&driverX("Speed")===(0.0),"stoping").otherwise("drive")).drop("del_Longitude").drop("del_Latitude").drop("del_Mileage")
    val stoprow = stop.withColumn("row_num",row_number().over(Window.partitionBy("SystemNo").orderBy("CurrentTime")))
    stoprow.createTempView("stp")


    val stpy = sc.sql("SELECT X.SystemNo,X.Longitude,X.Latitude,X.CurrentTime,X.stop as xstop,Y.stop as ystop FROM stp X LEFT JOIN stp Y ON X.SystemNo=Y.SystemNo AND X.row_num+1=Y.row_num ORDER BY X.row_num")

    val stpz = sc.sql("SELECT X.SystemNo,X.Longitude,X.Latitude,X.CurrentTime,X.stop as xstop,Y.stop as zstop FROM stp X LEFT JOIN stp Y ON X.SystemNo=Y.SystemNo AND X.row_num-1=Y.row_num ORDER BY X.row_num")

    val stopsum = stpy.join(stpz,Seq("SystemNo","Longitude","Latitude","CurrentTime","xstop"))
    val stopdrivestatus = stopsum.withColumn("status", when(stopsum("xstop") === ("stoping"), "stoping")
      .when(stopsum("xstop") === ("drive") && stopsum("ystop") === ("stoping") && stopsum("zstop") === ("drive"), "drive_end")
      .when(stopsum("xstop") === ("drive") && stopsum("ystop") === ("drive") && stopsum("zstop") === ("stoping"), "drive_start")
      .when(stopsum("xstop") === ("drive") && stopsum("ystop") === ("drive") && stopsum("zstop") === ("drive"), "drive")).drop("xstop").drop("ystop").drop("zstop")
    val DriveStartAndEnd = stopdrivestatus.join(driver,Seq("SystemNo","Longitude","Latitude","CurrentTime"))
    //.withColumn("hour",hour($"CurrentTime")).withColumn("minute",minute($"CurrentTime"))

    DriveStartAndEnd.withColumn("hour",hour(DriveStartAndEnd("CurrentTime"))).withColumn("minute",minute(DriveStartAndEnd("CurrentTime"))).repartition(1).write.parquet("/root/sparkdata/DriverStartAndEnd")


    //DriveStartAndEnd.repartition(1).write.parquet(basepath)
    sc.stop()
  }
}
