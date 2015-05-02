
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.PairRDDFunctions
import scala.util.Random

object GenerateGraph {


  def generateTruthFinder(sc: SparkContext, path: String, k:Int, numPartitions:Int): Graph[OProp, Double] = {
    println("GHEERERER4")
    val fileData = sc.textFile(path)//.collect.foreach(x => println("****" + x))//"file:///home/mustang/roadNet-CA.txt").collect.foreach(println)
    val parsedFileData = fileData.map(str => {
        val split = str.split('\t')
        val source = split(0)
        val obj = split(1)
        if(split.size < 3){
          (source, obj, Double.MaxValue)
        }else {
          var fact = split(2)
          if (!fact.isEmpty()) {
            val splits = fact.split("%")
            if (splits.size != 0) {
              fact = splits(0).stripPrefix("(").stripSuffix("%").stripSuffix(")").stripSuffix("%").stripSuffix(")")
              if (fact.isEmpty()) {
                (source, obj, Double.MaxValue)
              } else {
                (source, obj, fact.toDouble)
              }
            } else {
              (source, obj, Double.MaxValue)
            }
          } else {
            (source, obj, Double.MaxValue)
          }
        }

      }).collect.foreach(println)
    println("GHEERERER5")
    //val groundTruthFileData = sc.textFile(path+"-nasdaq-com")
    println("GHEERERER6")
    //fileData.collect.foreach(println)



    null
  }

  def generate(sc: SparkContext, k:Int, numPartitions:Int): Graph[VertexProperties, EdgeProperties] = {
    var g = GenerateGraph.readAndPreProcess(sc, k, numPartitions)
    //var g = generateToyGraph(sc, k, numPartitions)
    
    g.vertices.collect.foreach(v => {
      if (v == null) println(v)
      if (v._2 == null) println(v)
    })
    
    val countArray = g.vertices.aggregate(Array.ofDim[Int](k+1, 4))((a, b) => {
      var a1 = a.map(_.clone()) 
      a1(b._2.label.id)(b._2.vType.id) += 1
      a1
    }, (a1, a2) => {
      var a = a1.map(_.clone())
      // to k because of the NONE research area
      for(i <- 0 to k; j <- 0 to 3){
        a(i)(j) = a1(i)(j) + a2(i)(j)
      }
      a
    })
    //println(countArray.foreach(e => println(e.mkString(" "))))
    
    val oldg = g
    g = g.mapVertices((_, v1) => {
      var v = v1.copy()
      /*if(v.label == ResearchArea.NONE) {
        v.label = ResearchArea(Random.nextInt(k))
        v.rankDistribution.transform(x => 0.0)
        v.initialRankDistribution.transform(x => 0.0)
      }else{*/
        for (i <- 0 to 3) {
          if (v.label.id == i) {
            v.rankDistribution(i) = 1.0/countArray(i)(v.vType.id) 
            v.initialRankDistribution(i) = 1.0/countArray(i)(v.vType.id) 
          }
          else {
            v.rankDistribution(i) = 0.0
            v.initialRankDistribution(i) = 0.0
          }
        }
        //v.rankDistribution.transform(x => 1.0/countArray(v.label.id)(v.vType.id))
        //v.initialRankDistribution.transform(x => 1.0/countArray(v.label.id)(v.vType.id))
      //}
      v
    })
    //g.vertices.collect().foreach(e => println(e._2.initialRankDistribution.mkString(" ")))
    g
  }
  
  def generateToyGraph(sc: SparkContext, k:Int, numPartitions:Int): Graph[VertexProperties, EdgeProperties] = {
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
     
    g
  }


  def readAndPreProcess(sc: SparkContext, k:Int, numPartitions:Int): Graph[VertexProperties, EdgeProperties] = {
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
      //.repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Authors")
    val venueKeys = VertexRDD(sc.textFile(venueFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.VENUE, p, u.getOrElse(ResearchArea.NONE))
      )
      //.repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Venues")
    val termKeys = VertexRDD(sc.textFile(termFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.TERM, p, u.getOrElse(ResearchArea.NONE))
      )
      //.repartition(graph.vertices.partitions.length)
      .cache()
    println("Merged Terms")
    val paperKeys = VertexRDD(sc.textFile(paperFile).map(a => {
      val pair = a.split('\t')
      (pair(0).toLong, pair(1))
    })).leftJoin(confLabel)(
        (v, p, u) => new VertexProperties(k, VertexType.PAPER, p, u.getOrElse(ResearchArea.NONE))
      )
      //.repartition(graph.vertices.partitions.length)
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
      .filter(v => (v != null && v._2 != null))
    println("Joined objects and filtered invalid vertices")
        
    val newEdges = graph.edges.map(e => Edge(e.srcId, e.dstId, new EdgeProperties()))
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
    
    val edgesTemp = rankGraph.edges.filter(e => (e.attr != null))
    
    rankGraph = Graph(newVerts, edgesTemp)
    println("Number of partitions: " + rankGraph.edges.partitions.length)

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
