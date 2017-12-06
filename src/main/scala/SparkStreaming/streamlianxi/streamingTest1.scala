package SparkStreaming.streamlianxi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 12/5/17.
  */
object streamingTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDStream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))

    val lines = ssc.textFileStream("/root/spark-2.1.1/data/streaming/logtest")  //这里采用本地文件，当然你也可以采用HDFS文件
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
