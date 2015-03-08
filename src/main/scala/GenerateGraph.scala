
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.PairRDDFunctions

object GenerateGraph {



  def generate(sc: SparkContext, k:Int, numPartitions:Int): Graph[VertexProperties, EdgeProperties] = {
    var edgeFile = "data/dblp_hin/dblp_edgelist"
    var authorFile = "data/dblp_hin/author_key.txt"
    var venueFile = "data/dblp_hin/venue_key.txt"
    var termFile = "data/dblp_hin/term_key.txt"
    var paperFile = "data/dblp_hin/paper_key.txt"
    val confLabelFile = "data/DBLP_four_area/conf_label.txt"
    val authorLabelFile = "data/DBLP_four_area/author_label.txt"
    val termLabelFile = "data/DBLP_four_area/term_label.txt"
    val paperLabelFile = "data/DBLP_four_area/paper_label.txt"

    var graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, edgeFile, numEdgePartitions = numPartitions).cache()
    //println(s"*Edges: ${graph.edges.count}")
    //println(s"Vertices: ${graph.vertices.count}")

    val confLabel = VertexRDD(sc.textFile(confLabelFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, ResearchArea(pair(1).toInt))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Retrieved Conference Labels")
    val authorLabel = VertexRDD(sc.textFile(authorLabelFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong,  ResearchArea(pair(1).toInt))
    }))
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Retrieved Author Labels")
    /*
  val termLabel = VertexRDD(sc.textFile(termLabelFile).map(a=>{
    val pair = a.split('\t')
    (pair(0).toLong, ResearchArea(pair(1).toInt))
  }))
    .repartition(graph.vertices.partitions.length)
    .cache()
  println("Retrieved Term Labels")
  val paperLabel = VertexRDD(sc.textFile(paperLabelFile).map(a=>{
    val pair = a.split('\t')
    (pair(0).toLong, ResearchArea(pair(1).toInt))
  }))
    .repartition(graph.vertices.partitions.length)
    .cache()
  println("Retrieved Paper Labels")
  */
    val authorKeys = VertexRDD(sc.textFile(authorFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.AUTHOR, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Authors")
    val venueKeys = VertexRDD(sc.textFile(venueFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.VENUE, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Venues")
    val termKeys = VertexRDD(sc.textFile(termFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.TERM, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Terms")
    val paperKeys = VertexRDD(sc.textFile(paperFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.PAPER, p, u.getOrElse(ResearchArea.NONE))
      )
      .repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Papers")
    

    //println(s"Num authors: ${authorKeys.count()}")
    //println(s"Num venues: ${venueKeys.count()}")
    //println(s"Num terms: ${termKeys.count()}")
    //println(s"Num papers: ${paperKeys.count()}")
    var newVerts = graph.vertices
      .leftJoin(authorKeys)((v, i, u) => u.getOrElse(null))
      .leftJoin(venueKeys)((v, i, u) => u.getOrElse(i))
      .leftJoin(termKeys)((v, i, u) => u.getOrElse(i))
      .leftJoin(paperKeys)((v, i, u) => u.getOrElse(i))
      .filter(v => (v != null || v._2 != null))
    println("Joined objects and filtered invalid vertices")
    
    
    
    //confLabel.collect.foreach(println)
    val newEdges = newVerts.leftJoin(
      newVerts.leftJoin(graph.edges.map {
        case e => (e.srcId, e.dstId)
      }) {
        case (srcId, u, dstId) => (Some(dstId), srcId)
      }) {
        case (dstId, u, srcId) => Edge[EdgeProperties](Some(srcId), dstId, new EdgeProperties())
      //case (dstId, u, srcId) => Edge[EdgeProperties](srcId, dstId, new EdgeProperties())
    }
      
      /*.map {
        case (srcId, (u, dstId)) => (dstId, srcId)
      }).map {
        case (dstId, (u, srcId)) => Edge[EdgeProperties](srcId, dstId, new EdgeProperties())
      }*/
    //.map(e => Edge(e.srcId, e.dstId, new EdgeProperties())
        
    //val newEdges = graph.edges.map(e => Edge(e.srcId, e.dstId, new EdgeProperties()))
    var rankGraph = Graph(newVerts, newEdges)
    
    rankGraph = rankGraph.mapTriplets(e => {
      val e1 = e.copy()
      if (e.srcAttr == null || e.dstAttr == null) {
        null
      }
      else {
        e1.attr
      }
    }, TripletFields.All)
    
    //rankGraph.edges.collect().foreach(e => if (e.attr == null) println(e))
    //rankGraph.triplets.collect().foreach(e => if (e.srcAttr == null || e.dstAttr == null) println(e))
    val edgesTemp = newEdges.filter(e => (e.attr != null))
    
    rankGraph = Graph(newVerts, edgesTemp)
    //rankGraph.triplets.collect().foreach(e => if (e.srcAttr == null || e.dstAttr == null) println(e))
    val temp = rankGraph.triplets.filter(e => (e.srcAttr == null || e.dstAttr == null)).count()
    println(temp)
    /*
    val vertexOrdering = new Ordering[(VertexId, Double)] {
      override def compare(a: (VertexId, Double), b: (VertexId, Double)) = a._2.compare(b._2)
    }
    val now = System.nanoTime
    var rankGraph = Graph(newVerts, graph.edges)
    var ranks = rankGraph.staticPageRank(30).cache()
    ranks.edges.foreachPartition(x => {})
    val elapsed = System.nanoTime - now
    println("PageRank done: " + elapsed / 1000000000.0)
    val top = mutable.HashSet() ++ ranks.vertices.top(numTop)(vertexOrdering).map(_._1)
    val filtered = newVerts.filter(v => top.contains(v._1))
    println("Filtering complete")
    filtered.collect.foreach(println)

    //top.foreach(println)


    newVerts.collect.foreach(a => {
      if (a._2.label != ResearchArea.NONE) {
        println(a)
      }
      //println(s"${a._1} ${a._2}")
    })
    */
    rankGraph
  }

}
