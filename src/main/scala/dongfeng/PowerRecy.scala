package dongfeng

/**
  * Created by root on 10/23/17.
  */
import org.apache.spark.sql.SparkSession

object PowerRecy {
  def main(args: Array[String]): Unit = {
    if(args.length<3){
      """
        |
       """.stripMargin
      sys.exit(0)
    }

    val Array(readpath1,readpath2,basepath)=args


    val sc = SparkSession.builder().appName("PowerRecy").master("local").getOrCreate()
    //val sc = SparkSession.builder().appName("PowerRecy").getOrCreate()



    //val DriveSum =sc.read.parquet("/home/dongdou/sparkdataout/drivertime").groupBy("SystemNo","start_day","end_day").sum("delta_Soc","delta_milige","delta_time").toDF("SystemNo","start_day","end_day","Drive_sum_Soc","Drive_sum_milige","Drive_sum_time")
    val DriveSum =sc.read.parquet(readpath1).groupBy("SystemNo","start_day","end_day").sum("delta_Soc","delta_milige","delta_time").toDF("SystemNo","start_day","end_day","Drive_sum_Soc","Drive_sum_milige","Drive_sum_time")

    //val RecySum =sc.read.parquet("/home/dongdou/sparkdataout/RecyResult").groupBy("SystemNo","start_day","end_day").sum("delta_Soc","delta_milige","delta_time").toDF("SystemNo","start_day","end_day","Recy_sum_Soc","Recy_sum_milige","Recy_sum_time")
    val RecySum =sc.read.parquet(readpath2).groupBy("SystemNo","start_day","end_day").sum("delta_Soc","delta_milige","delta_time").toDF("SystemNo","start_day","end_day","Recy_sum_Soc","Recy_sum_milige","Recy_sum_time")



    val RecyPercent = DriveSum.join(RecySum, Seq("SystemNo", "start_day", "end_day"))



    RecyPercent.createTempView("recper")

    val RecyDriverPercent = sc.sql("SELECT SystemNo,start_day,end_day,Drive_sum_Soc,Drive_sum_milige,Drive_sum_time,Recy_sum_Soc,Recy_sum_milige," +
      "Recy_sum_time, (Recy_sum_Soc/Drive_sum_Soc) as soc_percent,(Recy_sum_milige/Drive_sum_milige)AS milige_percent,(Recy_sum_time/Drive_sum_time) AS time_percent FROM recper")
      .filter("soc_percent<1").filter("milige_percent<1").filter("time_percent<1")


    RecyDriverPercent.repartition(1).write.parquet(basepath)

  }
}
