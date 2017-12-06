package GraphExample

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 12/4/17.
  */
object PageRank2 {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("PageRank2").master("local").getOrCreate()

    import sc.implicits._

    val context = sc.sparkContext

    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(context,"/root/spark-2.1.1/data/graphx/followers.txt")

    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices

    val users = context.textFile("/root/spark-2.1.1/data/graphx/users.txt").map { line =>
      val fields = line.split(",")

      (fields(0).toLong, fields(1))

    }

    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    ccByUsername


    println(ccByUsername.collect().mkString("\n"))
    sc.stop()
  }
}
