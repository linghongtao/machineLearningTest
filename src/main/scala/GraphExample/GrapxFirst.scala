package GraphExample

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 12/4/17.
  */
object GrapxFirst {
  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("GrapxFirst").master("local").getOrCreate()

    import sc.stop

    val context = sc.sparkContext

    val users: RDD[(VertexId, (String, String))] =
      context.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      context.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    val vertice: VertexRDD[(String, String)] = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }

    val Vertexcount: VertexId = vertice.count
    // Count all the edges where src > dst
    val edges: RDD[Edge[String]] = graph.edges.filter(e => e.srcId > e.dstId)

    val edgecont = edges.count()
/*
    println(vertice.collect())
    println(Vertexcount)
    println(edges.collect())
    println(edgecont)*/

    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)

    facts.collect.foreach(println(_))


    sc.stop()
  }
}
