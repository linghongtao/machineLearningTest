package dongfeng

import org.apache.spark.sql._
import org.apache.spark.sql.functions.split

object pivotTable {
  def main(args: Array[String]): Unit = {
    if(args.length<2){
      """
        |
     """.stripMargin
      sys.exit(0)
    }

    val Array(readpath,basepath)=args
    //val sc = SparkSession.builder().appName("PivotTable").master("local").getOrCreate()
    val sc = SparkSession.builder().appName("PivotTable").getOrCreate()
    import sc.implicits._
    val rundata = sc.read.parquet(readpath)
    //val rundata = sc.read.parquet("/home/dongdou/sparkdata/p201512")
    val newrundata: DataFrame = rundata.withColumn("newCurrentValue",split($"CurrentValue","\\'").getItem(0)).filter("newCurrentValue>0").drop("CurrentValue")
    //newrundata.show(100)

    val rundatafil1 = newrundata.filter(!_.toString().contains("BatteryPack1MonomerVoltage")).filter(!_.toString().contains("BatteryPack1Probe")).filter(!_.toString().contains("CANNetworkFailure"))

     val rundatafil2 = rundatafil1.filter("SignalName='HCL_PTC_Temp' OR SignalName='VCU_PTCSwitch' OR SignalName='VCU_PTC_Run_Stop' OR SignalName='VCU_ACSwitch' " +
       "OR SignalName='BMS_SOC' OR SignalName='IC_Odmeter' OR SignalName='VCU_CruisingRange' OR SignalName='BMS_BatteryChargingTimes' " +
       "OR SignalName='BMS_ChargeFS' OR SignalName='BMS_ChargingTime' OR SignalName='BMS_ChargeSt' OR " +
       "SignalName='BMS_RemainingChargingTime' OR SignalName='Motor_OutputPower'  OR SignalName='Motor_Revolution' OR " +
       "SignalName='MotorTorque' OR SignalName='Motor_Temperature'")


    val signalname: Array[String] = rundatafil2 .select("SignalName").distinct().collect().map(_.getAs[String]("SignalName"))

    /* val wideDataColums: DataFrame = signalname.foldLeft(rundatafil2 ) {
       case (data, day) => data.selectExpr("*", s"IF(SignalName='$day',CurrentValue,) AS $day")
     }
     var diswidecolums = wideDataColums.drop("id").drop("SignalName").drop("CurrentValue").distinct()*/


    val wideDataColums: DataFrame = signalname.foldLeft(rundatafil1) {
      case (data, day) => data.selectExpr("*", s"IF(SignalName='$day',newCurrentValue,0) AS $day")
    }

    var diswidecolums = wideDataColums.drop("id").drop("SignalName").drop("newCurrentValue").distinct()
    //diswidecolums.show(100)
    import org.apache.spark.sql.functions._
    val toDouble = udf[Double,String](item=> item.toDouble)

    for(item<-signalname){
      val frame = diswidecolums.withColumn(item, toDouble(diswidecolums(item)))
      diswidecolums=frame
    }
    val cols: Array[String] = diswidecolums.schema.fieldNames

    val colNames: Array[Column] = cols.map(name=>col(name))
    val wd=diswidecolums.select(colNames:_*)

    val widedata = wd.groupBy("SystemNo", "Longitude", "Latitude", "Speed", "Direction", "Elevation",
      "Acc", "IsLocation", "Mileage", "CurrentTime").sum(signalname: _*) .toDF("SystemNo", "Longitude", "Latitude", "Speed", "Direction", "Elevation",
      "Acc", "IsLocation", "Mileage", "CurrentTime","BMS_SOC","BMS_ChargeSt",
      "Motor_OutputPower","VCU_CruisingRange","Motor_Temperature","BMS_ChargeFS","Motor_Revolution","IC_Odmeter")
    //wd.show(100)
    widedata.repartition(1).write.parquet(basepath)

    sc.stop()
  }
}
