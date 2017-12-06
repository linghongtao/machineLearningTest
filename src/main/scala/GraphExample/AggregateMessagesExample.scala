package GraphExample
import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
// $example off$
import org.apache.spark.sql.SparkSession
/**
  * Created by root on 11/22/17.
  */
object AggregateMessagesExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
        .master("local")
      .getOrCreate()


    val sc = spark.sparkContext

    // $example on$
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    // Compute the number of older followers and their total age

    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$

    spark.stop()
  }
}
