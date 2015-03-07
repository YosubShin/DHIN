
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
~/PredictionIO/vendors/spark-1.2.0/bin/spark-submit --class "SimpleApp" --master local[8] --driver-memory 6G --executor-memory 6G target/scala-2.10/DHIN_2.10-0.1-SNAPSHOT.jar
 */

import ResearchArea._
import VertexType._




object SimpleApp {

  def main(args: Array[String]) {
    println("Main")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("dhin"))
    //"spark://mustang12:7077", "DHIN", "/usr/local/Cellar/apache-spark/1.0.2/libexec",
      //List("target/scala-2.10/dhin_2.10-0.1-SNAPSHOT.jar"))

    //.partitionBy(PartitionStrategy.EdgePartition2D)

    var vertices = Array(
      (1L, VertexProperties(1, VertexType.PAPER, "", ResearchArea.AIML)),
      (2L, VertexProperties(2, VertexType.PAPER, "", ResearchArea.AIML)),
      (3L, VertexProperties(3, VertexType.AUTHOR, "", ResearchArea.AIML)),
      (4L, VertexProperties(4, VertexType.AUTHOR, "", ResearchArea.AIML)),
      (5L, VertexProperties(5, VertexType.VENUE, "", ResearchArea.AIML)),
      (6L, VertexProperties(6, VertexType.VENUE, "", ResearchArea.AIML)),
      (7L, VertexProperties(7, VertexType.TERM, "", ResearchArea.AIML)),
      (8L, VertexProperties(8, VertexType.TERM, "", ResearchArea.AIML))
    )

    var edges = Array(
      Edge(1L, 3L, EdgeProperties()),
      Edge(1L, 5L, EdgeProperties()),
      Edge(1L, 7L, EdgeProperties()),
      Edge(2L, 4L, EdgeProperties()),
      Edge(2L, 6L, EdgeProperties()),
      Edge(2L, 8L, EdgeProperties()),
      Edge(1L, 2L, EdgeProperties())
    )

    val g: Graph[VertexProperties, EdgeProperties] = Graph(sc.parallelize(vertices), sc.parallelize(edges))


    val aggregateTypes = g.aggregateMessages[Array[Double]](
      ctx => {
        ctx.sendToDst({
          val arr = Array.ofDim[Double](4)
          arr(ctx.srcAttr.label.id) = ctx.attr.R
          arr
        })
        ctx.sendToSrc({
          val arr = Array.ofDim[Double](4)
          arr(ctx.dstAttr.label.id) = ctx.attr.R
          arr
        })
      },
      (a1, a2) => a1.zip(a2).map(a => a._1 + a._2)
    )

    val numPartitions = 16
    val numTop = 100
    val k: Int = 4

    //val graph = GenerateGraph.generate(sc, k, numPartitions)



    sc.stop()
  }

}
