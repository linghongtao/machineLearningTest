package GraphExample

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 12/4/17.
  */
object PageRank1 {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("PageRank1").master("local").getOrCreate()

    import sc.implicits._

    val context = sc.sparkContext

    val graph = GraphLoader.edgeListFile(context,"/root/spark-2.1.1/data/graphx/followers.txt")

    val ranks = graph.pageRank(0.0001).vertices

    val users = context.textFile("/root/spark-2.1.1/data/graphx/users.txt").map { line =>
      val fields = line.split(",")

      (fields(0).toLong, fields(1))

    }
    val RanksByusername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    println(RanksByusername.collect().mkString("\n"))


    sc.stop()
  }
}
