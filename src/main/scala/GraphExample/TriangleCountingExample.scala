package GraphExample

import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 12/5/17.
  */
object TriangleCountingExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load the edges in canonical order and partition the graph for triangle count
    val graph = GraphLoader.edgeListFile(sc, "/root/spark-2.1.1/data/graphx/followers.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = sc.textFile("/root/spark-2.1.1/data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
  }
}
