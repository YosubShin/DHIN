
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

object RankClass {

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

    val numClasses = ResearchArea.values.size - 1
    val numTypes = VertexType.values.size
    val numBuildNetworkIterations = 5
    val numEmIterations = 5
    val numPartitions = 32
    val numTop = 100

    val g = GenerateGraph.generate(sc, numClasses, numPartitions).partitionBy(PartitionStrategy.EdgePartition2D).cache()

    val lambda = Array.ofDim[Double](numTypes, numTypes).transform(x => x.transform(y => 0.2).array).array
    val alpha = Array.ofDim[Double](numTypes).transform(x => 0.1).array

    val now = System.nanoTime
    println("Starting AuthorityRank")
    val ranks = IterativeNetworkConstruction.run(sc, g, numBuildNetworkIterations, lambda, alpha, numTypes, numClasses)
    ranks.edges.foreachPartition(x => {})
    val elapsed = System.nanoTime - now
    println("AuthorityRank completed in : " + elapsed / 1000000000.0 + " seconds")

    for(i <- 0 until numClasses){
      val ordering = new Ordering[(VertexId, VertexProperties)] {
        override def compare(a: (VertexId, VertexProperties), b: (VertexId, VertexProperties)) = {
          val s: Double = a._2.rankDistribution(i)
          val t: Double = b._2.rankDistribution(i)
          s.compare(t)
        }
      }

      println(s"Top overall elements for ${ResearchArea(i)}")
      val top = ranks.vertices.top(10)(ordering)
      top.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
      println(s"Top authors for ${ResearchArea(i)}")
      val topAuthors = ranks.vertices.filter(e => (e._2.vType == VertexType.AUTHOR)).top(10)(ordering)
      topAuthors.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
      println(s"Top venues for ${ResearchArea(i)}")
      val topVenues = ranks.vertices.filter(e => (e._2.vType == VertexType.VENUE)).top(10)(ordering)
      topVenues.foreach(x => println(s"${x._1} ${x._2.attribute} ${x._2.vType} ${x._2.rankDistribution.mkString(" ")}"))
      println(s"************************************")
    }

    val finalRelativeSizesOfClassesForTypes = EM.run(sc, ranks, numEmIterations, numTypes, numClasses)

    sc.stop()
  }

}
