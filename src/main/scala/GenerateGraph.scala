
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD, PairRDDFunctions}
import scala.collection.mutable
import scala.util.Random

object GenerateGraph {

  def parseStockFile(sc: SparkContext, path: String, numPartitions:Int) : RDD[(String, String, Double)] = {
    val fileData = sc.textFile(path)//.collect.foreach(x => println("****" + x))//"file:///home/mustang/roadNet-CA.txt").collect.foreach(println)
    fileData.map(str => {
      val split = str.split('\t')
      val source = split(0)
      val obj = split(1)
      if(split.size < 3){
        (source, obj, Double.MaxValue)
      }else {
        var fact = split(2)
        if (!fact.isEmpty()) {
          var splits = fact.split("%")
          if (splits.size != 0) {
            splits = splits(0).split("u")
            if (splits.size != 0) {
              fact = splits(0).stripPrefix("(").stripSuffix("%").stripSuffix(")").stripSuffix("%").stripSuffix(")").stripSuffix("-")
              if (fact.isEmpty()) {
                (source, obj, Double.MaxValue)
              } else {
                (source, obj, fact.toDouble)
              }
            }else{
              (source, obj, Double.MaxValue)
            }
          } else {
            (source, obj, Double.MaxValue)
          }
        } else {
          (source, obj, Double.MaxValue)
        }
      }
    })
  }

  def parseStockFileGT(sc: SparkContext, path: String, numPartitions:Int) : RDD[(String, Double)] = {
    val fileData = sc.textFile(path)//.collect.foreach(x => println("****" + x))//"file:///home/mustang/roadNet-CA.txt").collect.foreach(println)
    fileData.map(str => {
      val split = str.split('\t')
      val obj = split(0)
      if(split.size < 2){
        (obj, Double.MaxValue)
      }else {
        var fact = split(1)
        if (!fact.isEmpty()) {
          var splits = fact.split("%")
          if (splits.size != 0) {
            splits = splits(0).split("u")
            if (splits.size != 0) {
              fact = splits(0).stripPrefix("(").stripSuffix("%").stripSuffix(")").stripSuffix("%").stripSuffix(")").stripSuffix("-")
              if (fact.isEmpty()) {
                (obj, Double.MaxValue)
              } else {
                (obj, fact.toDouble)
              }
            }else{
              (obj, Double.MaxValue)
            }
          } else {
            (obj, Double.MaxValue)
          }
        } else {
          (obj, Double.MaxValue)
        }
      }
    })
  }


  def stringHash(str: String) : VertexId = {
    str.toLowerCase.replace(" ", "").hashCode.toLong
  }


  def generateTruthFinder(sc: SparkContext, path: String, numPartitions:Int): (Graph[OProp, Double], VertexRDD[Double]) = {

    val data = parseStockFile(sc, path, numPartitions)

    // vertices is either (hash of source, default value), or (hash of object, fact)

    //data.collect.foreach(x => println(x._2 + x._3.toString()))

    val vertices = VertexRDD(data.map(x => (stringHash(x._1), OProp(VType.WEBSITE, 0.9, x._1)))
      .union(data.map(x => (stringHash(x._2 + x._3.toString()), OProp(VType.FACT, x._3, x._2)))))
      .union(data.map(x => (stringHash(x._2), OProp(VType.OBJECT, 0.0, x._2))))
      .repartition(numPartitions)

    /*val vertices = VertexRDD(data.map(x => (stringHash(x._1), OProp(VType.WEBSITE, 0.9, x._1)))
      .union(data.map(x => (stringHash(x._2), OProp(VType.FACT, x._3, x._2)))))
      .repartition(numPartitions)
    */
    val edges = data.map(x => Edge(stringHash(x._1), stringHash(x._2 + x._3.toString()), 0.0))
      .union(data.map(x => Edge(stringHash(x._2 + x._3.toString()), stringHash(x._2), 0.0)))
      .repartition(numPartitions)

    val graph = Graph[OProp, Double](vertices, edges)


    val groundTruth = VertexRDD(parseStockFileGT(sc, path+"-nasdaq-com", numPartitions)
      .map(x => (stringHash(x._1 + x._2.toString()), x._2)).repartition(numPartitions))

    /*val verticesGT = VertexRDD(groundTruth.map(x => (stringHash(x._1), OProp(VType.WEBSITE, 0.9)))
      .union(groundTruth.map(x => (stringHash(x._2), OProp(VType.FACT, x._3)))))
    val edgesGT = groundTruth.map(x => Edge(stringHash(x._1), stringHash(x._2), 0.0))

    val graphGT = Graph[OProp, Double](verticesGT, edgesGT)

    graphGT.edges.collect.foreach(println)
    println(graphGT.edges.count)
    */
    /*
    groundTruth.collect.foreach(println)
    println(groundTruth.count)
    println(graph.edges.count)
    */
    (graph, groundTruth)//, graphGT)
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
