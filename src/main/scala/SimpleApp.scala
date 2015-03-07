
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
~/PredictionIO/vendors/spark-1.2.0/bin/spark-submit --class "SimpleApp" --master local[8] --driver-memory 6G --executor-memory 6G target/scala-2.10/DHIN_2.10-0.1-SNAPSHOT.jar
 */


object ResearchArea extends Enumeration {
  type ResearchArea = Value
  val DATABASES, AIML, DATA_MINING, IR, NONE = Value
}

object VertexType extends Enumeration {
  type VertexType = Value
  val PAPER, VENUE, AUTHOR, TERM = Value
}

class VertexProperties(t: VertexType.VertexType, attr: String, l: ResearchArea.ResearchArea){
  val vType = t
  val attribute = attr
  var label: ResearchArea.ResearchArea = l
}

def intToRA(i: Int): ResearchArea.ResearchArea ={
  if(i == 0){
    ResearchArea.DATABASES
  }else if(i == 1){
    ResearchArea.AIML
  }else if(i == 2){
    ResearchArea.DATA_MINING
  }else if(i == 3){
    ResearchArea.IR
  }else{
    ResearchArea.NONE
  }
}

object SimpleApp {

  def main(args: Array[String]) {
    println("Main")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("dhin"))
    //"spark://mustang12:7077", "DHIN", "/usr/local/Cellar/apache-spark/1.0.2/libexec",
      //List("target/scala-2.10/dhin_2.10-0.1-SNAPSHOT.jar"))
    var edgeFile = "data/dblp_hin/dblp_edgelist"
    var authorFile = "data/dblp_hin/author_key.txt"
    var venueFile = "data/dblp_hin/venue_key.txt"
    var termFile = "data/dblp_hin/term_key.txt"
    var paperFile = "data/dblp_hin/paper_key.txt"
    val confLabelFile = "data/DBLP_four_area/conf_label.txt"
    val authorLabelFile = "data/DBLP_four_area/author_label.txt"
    val termLabelFile = "data/DBLP_four_area/term_label.txt"
    val paperLabelFile = "data/DBLP_four_area/paper_label.txt"
    //.partitionBy(PartitionStrategy.EdgePartition2D)

    val numPartitions = 16
    val numTop = 100
    var graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFile, numEdgePartitions=numPartitions).cache()
    println(s"*Edges: ${graph.edges.count}")
    println(s"Vertices: ${graph.vertices.count}")

    val confLabel = VertexRDD(sc.textFile(confLabelFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, intToRA(pair(1).toInt))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Retrieved Conference Labels")
    val authorLabel = VertexRDD(sc.textFile(authorLabelFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, intToRA(pair(1).toInt))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Retrieved Author Labels")
    val termLabel = VertexRDD(sc.textFile(termLabelFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, intToRA(pair(1).toInt))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Retrieved Term Labels")
    val paperLabel = VertexRDD(sc.textFile(paperLabelFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, intToRA(pair(1).toInt))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Retrieved Paper Labels")

    val authorKeys = VertexRDD(sc.textFile(authorFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(VertexType.AUTHOR, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Authors")
    val venueKeys = VertexRDD(sc.textFile(venueFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(VertexType.VENUE, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Venues")
    val termKeys = VertexRDD(sc.textFile(termFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(VertexType.TERM, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Terms")
    val paperKeys = VertexRDD(sc.textFile(paperFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(VertexType.PAPER, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Papers")

    println(s"Num authors: ${authorKeys.count()}")
    println(s"Num venues: ${venueKeys.count()}")
    println(s"Num terms: ${termKeys.count()}")
    println(s"Num papers: ${paperKeys.count()}")
    var newVerts = graph.vertices
      .leftJoin(authorKeys)((v, i, u) => u.getOrElse(null))
      .leftJoin(venueKeys)((v, i, u) => u.getOrElse(i))
      .leftJoin(termKeys)((v, i, u) => u.getOrElse(i))
      .leftJoin(paperKeys)((v, i, u) => u.getOrElse(i))
      .filter(v => v._2 != null)
    println("Joined objects and filtered invalid vertices")

    //confLabel.collect.foreach(println)
    /*
    val vertexOrdering = new Ordering[(VertexId, Double)] {
      override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
    }
    val now = System.nanoTime
    var rankGraph = Graph(newVerts, graph.edges)
    var ranks = rankGraph.staticPageRank(30).cache()
    ranks.edges.foreachPartition(x => {})
    val elapsed = System.nanoTime - now
    println("PageRank done: " + elapsed/1000000000.0)
    val top = mutable.HashSet() ++ ranks.vertices.top(numTop)(vertexOrdering).map(_._1)
    val filtered = newVerts.filter(v => top.contains(v._1))
    println("Filtering complete")
    filtered.collect.foreach(println)
    */
    //top.foreach(println)
    /*
    newVerts.collect.foreach(a => {
      println(s"${a._1} ${a._2}")
    })
    */
    sc.stop()
  }

}
