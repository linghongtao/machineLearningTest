package GraphExample

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession
import shapeless.ops.hlist.Partition

/**
  * Created by root on 12/4/17.
  */
object PageRank3 {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("PageRank3").master("local").getOrCreate()

    import sc.implicits._

    val context = sc.sparkContext

    val graph = GraphLoader.edgeListFile(context, "/root/spark-2.1.1/data/graphx/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    val triCounts = graph.triangleCount().vertices

    val users = context.textFile("/root/spark-2.1.1/data/graphx/users.txt").map { line =>
      val fields = line.split(",")

      (fields(0).toLong, fields(1))

    }


    val usernametc = users.join(triCounts).map {
      case (id, (username, tc)) => (username, tc)
    }

    println(usernametc.collect().mkString("\n"))

    sc.stop()
  }
}
