package XMC

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by root on 12/5/17.
  */
object transpose {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("").master("local").getOrCreate()

    import session.implicits._

    val df = Seq(
      (1, 100, 0, 0, 0, 0, 0),
      (2, 0, 50, 0, 0, 20, 0),
      (3, 0, 0, 0, 0, 0, 0),
      (4, 0, 0, 0, 0, 0, 0)
    ).toDF("segment_id", "val1", "val2", "val3", "val4", "val5", "val6")

    val sc = session.sparkContext

    val d = df.select("segment_id", "val1")


    val tf = d.groupBy("val1").sum().withColumnRenamed("val1", "vals")

    //tf.show()

    val (header, data) = df.collect.map(_.toSeq.toArray).transpose match {
      case Array(h, t @ _*) => {
        (h.map(_.toString), t.map(_.collect { case x: Int => x }))
      }
    }


    val rows = df.columns.tail.zip(data).map { case (x, ys) => Row.fromSeq(x +: ys) }


    val schema = StructType(
      StructField("vals", StringType) +: header.map(StructField(_, IntegerType))
    )

    val transpose = session.createDataFrame(sc.parallelize(rows), schema)

    transpose.show()
    session.stop()
  }
}
