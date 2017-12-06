package GraphExample

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 12/5/17.
  */
object ConnectedComponentsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    // $example on$
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "/root/spark-2.1.1/data/graphx/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("/root/spark-2.1.1/data/graphx/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
  }
}
