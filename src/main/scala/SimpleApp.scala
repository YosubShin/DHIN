
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
//    val conf = new SparkConf().setMaster("spark://mustang12:7077").setAppName("dhin")

    //val sc = new SparkContext(conf.setAppName("dhin"))
    val sc = new SparkContext("local[8]", "DHIN", "/usr/local/Cellar/apache-spark/1.2.1/libexec",
      List("target/scala-2.10/dhin_2.10-0.1-SNAPSHOT.jar"))

//    val sc = new SparkContext(conf)


    /*val sc = new SparkContext("local[1]", "DHIN", "$SPARK_HOME/libexec",
      List("target/scala-2.10/dhin_2.10-0.1-SNAPSHOT.jar"))
    */
    /*"spark://mustang12:7077"*/
//    sc.setCheckpointDir("/home/mustang/tmp")
    sc.setCheckpointDir("/tmp")
//    .partitionBy(PartitionStrategy.EdgePartition2D)

    val k: Int = 4
    val numPartitions = 32
    val numTop = 100
//    val (graph, gt) = GenerateGraph.generateTruthFinder(sc, "/home/mustang/clean_stocks/data", 16)
    //graph.vertices.collect.foreach(println)
//    val g = TruthFinder.runSingleFact(sc, graph, 1)
//    g.edges.foreach(x => {})
//    g.vertices.collect.foreach(println)
    //stock-2011-07-01

    val g = GenerateGraph.generate(sc, k, numPartitions).partitionBy(PartitionStrategy.EdgePartition2D).cache()

    val lambda = Array.ofDim[Double](4, 4).transform(x => x.transform(y => 0.2).array).array
    var alpha = Array.ofDim[Double](4).transform(x => 0.1).array
    val now = System.nanoTime
    println("Starting AuthorityRank")
    var ranks = AuthorityRank.run(sc, g, 5, lambda, alpha)
    ranks.edges.foreachPartition(x => {})
    val elapsed = System.nanoTime - now
    println("AuthorityRank completed in : " + elapsed / 1000000000.0 + " seconds")

    for(i <- 0 to 3){
      val ordering = new Ordering[(VertexId, VertexProperties)] {
        override def compare(a: (VertexId, VertexProperties), b: (VertexId, VertexProperties)) = {
          var s: Double = a._2.rankDistribution(i)
          var t: Double = b._2.rankDistribution(i)
          s.compare(t)
        }
      }

      println(s"Top overall elements for ${ResearchArea(i)}")
      var top = ranks.vertices.top(10)(ordering)
      top.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
      println(s"Top authors for ${ResearchArea(i)}")
      var topAuthors = ranks.vertices.filter(e => (e._2.vType == VertexType.AUTHOR)).top(10)(ordering)
      topAuthors.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
      println(s"Top venues for ${ResearchArea(i)}")
      var topVenues = ranks.vertices.filter(e => (e._2.vType == VertexType.VENUE)).top(10)(ordering)
      topVenues.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
      println(s"************************************")

      val initialRelativeSizesOfClassesForTypes = Array.ofDim[Double](4, 4).transform(x => x.transform(y => 1.0 / k).array).array
      val finalRelativeSizesOfClassesForTypes = EM.run(sc, ranks, 5, initialRelativeSizesOfClassesForTypes)
    }

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
    /*
    val vertexOrdering = new Ordering[(VertexId, Double)] {
      override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
    }
    val now = System.nanoTime
    var ranks = g.staticPageRank(5).cache()
    ranks.edges.foreachPartition(x => {})
    val elapsed = System.nanoTime - now
    println("PageRank done: " + elapsed / 1000000000.0)
    val top = Set() ++ ranks.vertices.top(numTop)(vertexOrdering).map(_._1)
    top.foreach(println)
    //val filtered = newVerts.filter(v => top.contains(v._1))
    //filtered.collect.foreach(println)

    //top.foreach(println)
    */
    /*
    val vertexOrdering = new Ordering[(VertexId, Double)] {
      override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
    }
  */
    /*
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
    var top1 = ranks.vertices.top(10)(vertexOrdering2)//.map(_._1)
    top1.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))

    var topAuthors = ranks.vertices.filter(e => (e._2.vType == VertexType.AUTHOR)).top(10)(vertexOrdering2)
    topAuthors.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
    */
    sc.stop()
  }

}
