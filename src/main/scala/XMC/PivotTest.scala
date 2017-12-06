package XMC

import org.apache.spark.sql.SparkSession

/**
  * Created by root on 11/23/17.
  */
object PivotTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("PivotTest").master("local").getOrCreate()

    import sc.implicits._

    val zhaibiao = sc.read.option("header","true").csv("/root/sparkdata/zhaibiaonew.csv")

    val widedata = zhaibiao.groupBy("id").pivot("item").agg("value"->"sum").na.fill("")

    widedata.show(100)

    sc.stop()
  }
}
