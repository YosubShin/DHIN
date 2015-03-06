import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/*
~/PredictionIO/vendors/spark-1.2.0/bin/spark-submit --class "SimpleApp" --master local[8] --driver-memory 6G --executor-memory 6G target/scala-2.10/DHIN_2.10-0.1-SNAPSHOT.jar
 */

object SimpleApp {

  object VertexType extends Enumeration{
    type VertexType = Value
    val PAPER, VENUE, AUTHOR, TERM = Value
  }

  def main(args: Array[String]) {
    println("Main")
    //Logger.getLogger("org").setLevel(Level.WARN)
    //Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf()
    val sc = new SparkContext(conf.setAppName("dhin"))
    //"spark://mustang12:7077", "DHIN", "/usr/local/Cellar/apache-spark/1.0.2/libexec",
      //List("target/scala-2.10/dhin_2.10-0.1-SNAPSHOT.jar"))
    var edgeFile = "data/dblp_hin/dblp_edgelist"
    var authorFile = "data/dblp_hin/author_key.txt"
    var venueFile = "data/dblp_hin/venue_key.txt"
    var termFile = "data/dblp_hin/term_key.txt"
    var paperFile = "data/dblp_hin/paper_key.txt"
    //.partitionBy(PartitionStrategy.EdgePartition2D)

    val numPartitions = 16
    var graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFile, numEdgePartitions=numPartitions).cache()
    println(s"*Edges: ${graph.edges.count}")
    println(s"Vertices: ${graph.vertices.count}")

    println(graph.edges.partitions.length)
    println(graph.vertices.partitions.length)


    val authorKeys = VertexRDD(sc.textFile(authorFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, VertexType.AUTHOR)//pair(1))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()

    val venueKeys = VertexRDD(sc.textFile(venueFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, VertexType.VENUE)//pair(1))
    })).repartition(graph.vertices.partitions.length)
      .cache()
    val termKeys = VertexRDD(sc.textFile(termFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, VertexType.TERM)//pair(1))
    })).repartition(graph.vertices.partitions.length)
      .cache()
    val paperKeys = VertexRDD(sc.textFile(paperFile).map(a=>{
      val pair = a.split('\t')
      (pair(0).toLong, VertexType.PAPER)//pair(1))
    })).repartition(graph.vertices.partitions.length)
      .cache()

    println(authorKeys.count())
    println(venueKeys.count())
    println(termKeys.count())
    println(paperKeys.count())


    var newVerts = graph.vertices.leftJoin(authorKeys)((v, i, u) => u.getOrElse(null))
  /*
    newVerts.collect.foreach(a =>{
      if(a._2 != null){
        println(s"${a._1} ${a._2}")
      }
    })
    */

    newVerts = newVerts.leftJoin(venueKeys)((v, i, u) => u.getOrElse(i))
    newVerts = newVerts.leftJoin(termKeys)((v, i, u) => u.getOrElse(i))
    newVerts = newVerts.leftJoin(paperKeys)((v, i, u) => u.getOrElse(i))
    newVerts.collect.foreach(a => {
      println(s"${a._1} ${a._2}")
    })

    sc.stop()
  }

}
