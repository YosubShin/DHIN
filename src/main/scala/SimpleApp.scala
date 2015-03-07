
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkException
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.graphx.lib._

/*
~/PredictionIO/vendors/spark-1.2.0/bin/spark-submit --class "SimpleApp" --master local[8] --driver-memory 6G --executor-memory 6G target/scala-2.10/DHIN_2.10-0.1-SNAPSHOT.jar
 */

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

    val k: Int = 4
    val numPartitions = 16
    val numTop = 100

    var vertices = Array(
      (1L, VertexProperties(k, VertexType.PAPER, "", ResearchArea.DATA_MINING)),
      (2L, VertexProperties(k, VertexType.PAPER, "", ResearchArea.AIML)),
      (3L, VertexProperties(k, VertexType.AUTHOR, "", ResearchArea.DATA_MINING)),
      (4L, VertexProperties(k, VertexType.AUTHOR, "", ResearchArea.NONE)),
      (5L, VertexProperties(k, VertexType.VENUE, "", ResearchArea.NONE)),
      (6L, VertexProperties(k, VertexType.VENUE, "", ResearchArea.AIML)),
      (7L, VertexProperties(k, VertexType.TERM, "", ResearchArea.DATA_MINING)),
      (8L, VertexProperties(k, VertexType.TERM, "", ResearchArea.AIML)),
      (9L, VertexProperties(k, VertexType.AUTHOR, "", ResearchArea.NONE)),
      (10L, VertexProperties(k, VertexType.AUTHOR, "", ResearchArea.DATA_MINING))
    )

    var edges = Array(
      Edge(1L, 3L, EdgeProperties()),
      Edge(1L, 5L, EdgeProperties()),
      Edge(1L, 7L, EdgeProperties()),
      Edge(2L, 4L, EdgeProperties()),
      Edge(2L, 6L, EdgeProperties()),
      Edge(2L, 8L, EdgeProperties()),
      Edge(1L, 2L, EdgeProperties()),
      Edge(1L, 9L, EdgeProperties()),
      Edge(1L, 10L, EdgeProperties())
    )

    var g: Graph[VertexProperties, EdgeProperties] = Graph(
      sc.parallelize(vertices),
      sc.parallelize(edges))

    val countArray = g.vertices.aggregate(Array.ofDim[Int](k+1, 4))((a, b) => {
      a(b._2.label.id)(b._2.vType.id) += 1
      a
    }, (a1, a2) => {
      // to k because of the NONE research area
      for(i <- 0 to k; j <- 0 to 3){
        a1(i)(j) += a2(i)(j)
      }
      a1
    })
    val oldg = g
    g = g.mapVertices((_, v) => {
      if(v.label == ResearchArea.NONE) {
        v.label = ResearchArea(Random.nextInt(k))
        v.rankDistribution.transform(x => 0.0)
        v.initialRankDistribution.transform(x => 0.0)
      }else{
        v.rankDistribution.transform(x => 1.0/countArray(v.label.id)(v.vType.id))
        v.initialRankDistribution.transform(x => 1.0/countArray(v.label.id)(v.vType.id))
      }
      v
    })
    g.unpersist(false)

    /*
    val aggregateTypes:VertexRDD[Array[Double]] = g.aggregateMessages[Array[Double]](
      ctx => {
        ctx.sendToDst({
          val arr = Array.ofDim[Double](4)
          arr(ctx.srcAttr.vType.id) = ctx.attr.R
          arr
        })
        ctx.sendToSrc({
          val arr = Array.ofDim[Double](4)
          arr(ctx.dstAttr.vType.id) = ctx.attr.R
          arr
        })
      },
      (a1, a2) => a1.zip(a2).map(a => a._1 + a._2),
      TripletFields.All
    )
    //var newGraph: Graph[VertexProperties, EdgeProperties] = g.joinVertices(aggregateTypes)((a, b, c) => )
    var newGraph = Graph(
      g.vertices.leftJoin(aggregateTypes)((a, b, c) => (b, c.getOrElse(Array[Double]()))),
      g.edges
    ).mapTriplets(triplet => {
      val e = EdgeProperties()
      val srcSum = triplet.srcAttr._2(triplet.dstAttr._1.vType.id)
      val dstSum = triplet.dstAttr._2(triplet.srcAttr._1.vType.id)
      val r = triplet.attr.R
      e.S = (1.0/math.sqrt(srcSum))*r*(1.0/math.sqrt(dstSum))
      e
    })


    //aggregateTypes.collect.foreach(a => {a._2.foreach(a_ => print(s"${a_} ")); println})
    newGraph.edges.collect.foreach(a => println(s"${a.srcId} ${a.dstId} ${a.attr.S}"))
    //AuthorityRank.run(g)
    */


    val vertexOrdering = new Ordering[(VertexId, Double)] {
      override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
    }

    val vertexOrdering1 = new Ordering[(VertexId, VertexProperties)] {
      override def compare(a: (VertexId, VertexProperties), b: (VertexId, VertexProperties)) = {
        var s: Double = a._2.rankDistribution(0)
        var t: Double = b._2.rankDistribution(0)
        s.compare(t)
      }
    }


    val vertexOrdering2 = new Ordering[(VertexId, VertexProperties)] {
      override def compare(a: (VertexId, VertexProperties), b: (VertexId, VertexProperties)) = {
        var s: Double = a._2.rankDistribution(1)
        var t: Double = b._2.rankDistribution(1)
        s.compare(t)
      }
    }

    val vertexOrdering3 = new Ordering[(VertexId, VertexProperties)] {
      override def compare(a: (VertexId, VertexProperties), b: (VertexId, VertexProperties)) = {
        var s: Double = a._2.rankDistribution(2)
        var t: Double = b._2.rankDistribution(2)
        s.compare(t)
      }
    }

    val vertexOrdering4 = new Ordering[(VertexId, VertexProperties)] {
      override def compare(a: (VertexId, VertexProperties), b: (VertexId, VertexProperties)) = {
        var s: Double = a._2.rankDistribution(3)
        var t: Double = b._2.rankDistribution(3)
        s.compare(t)
      }
    }

    val lambda = Array.ofDim[Double](4, 4).transform(x => x.transform(y => 0.2).array).array
    var alpha = Array.ofDim[Double](4).transform(x => 0.1).array

    var ranks = AuthorityRank.run(g, 2, lambda, alpha)


    var top1 = ranks.vertices.top(10)(vertexOrdering1)//.map(_._1)
    top1.foreach(x => println(s"${x._1} ${x._2.rankDistribution.mkString(" ")}"))




    //val graph = GenerateGraph.generate(sc, k, numPartitions)



    sc.stop()
  }

}
