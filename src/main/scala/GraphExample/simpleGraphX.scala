package GraphExample

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by root on 12/4/17.
  */
object simpleGraphX {
  def main(args: Array[String]): Unit = {
    val sparksession = SparkSession.builder().appName("simpleGraphX").master("local").getOrCreate()

    import  sparksession.implicits._

    val sc = sparksession.sparkContext

    // 顶点
    val vertexArray = Array(
      (1L,("Alice", 38)),
      (2L,("Henry", 27)),
      (3L,("Charlie", 55)),
      (4L,("Peter", 32)),
      (5L,("Mike", 35)),
      (6L,("Kate", 23))
    )

    // 边
    val edgeArray = Array(
      Edge(2L, 1L, 5),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 7),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 3),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 8)
    )


    //构造vertexRDD和edgeRDD
    val vertexRDD:RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
    val edgeRDD:RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // 构造图
    val graph:Graph[(String,Int),Int] = Graph(vertexRDD, edgeRDD)


    //图的属性操作
    println("*************************************************************")
    println("属性演示")
    println("*************************************************************")
    // 方法一
    println("找出图中年龄大于20的顶点方法之一:")
    graph.vertices.filter{case(id,(name,age)) => age>20}.collect.foreach {
      case(id,(name,age)) => println(s"$name is $age")
    }

    // 方法二
    println("找出图中年龄大于20的顶点方法之二:")
    graph.vertices.filter(v => v._2._2>20).collect.foreach {
      v => println(s"${v._2._1} is ${v._2._2}")
    }

    // 边的操作
    println("找出图中属性大于3的边:")
    graph.edges.filter(e => e.attr>3).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    println

    // Triplet操作
    println("列出所有的Triples:")
    for(triplet <- graph.triplets.collect){
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    println("列出边属性>3的Triples:")
    for(triplet <- graph.triplets.filter(t => t.attr > 3).collect){
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }
    println

    // Degree操作
    println("找出图中最大的出度,入度,度数:")
    def max(a:(VertexId,Int), b:(VertexId,Int)):(VertexId,Int) = {
      if (a._2>b._2) a else b
    }
    println("Max of OutDegrees:" + graph.outDegrees.reduce(max))
    println("Max of InDegrees:" + graph.inDegrees.reduce(max))
    println("Max of Degrees:" + graph.degrees.reduce(max))
    println
    sparksession.stop()
  }
}
