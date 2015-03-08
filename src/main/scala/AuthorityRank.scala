
import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.graphx._
import org.apache.spark.graphx.TripletFields
import org.apache.spark.SparkException
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object AuthorityRank extends Logging {

  def run(graph: Graph[VertexProperties, EdgeProperties], numIter: Int, lambda: Array[Array[Double]], alpha: Array[Double])
  : Graph[VertexProperties, EdgeProperties] = {
    val rankDenominator = lambda.zip(alpha).map(x => x._1.sum + x._2)
    var rankGraph = graph
    var iteration = 1
    var prevRankGraph: Graph[VertexProperties, EdgeProperties] = null

    graph.checkpoint()

    while (iteration < numIter) {
      rankGraph.cache()
      // Vertices collecting R[i,j] values from neighbors
      println("Start 1")
      var now = System.nanoTime
      val aggregateTypes:VertexRDD[Array[Double]] = rankGraph.aggregateMessages[Array[Double]](
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
      ).cache()
      aggregateTypes.foreachPartition(x => {})
      var elapsed = System.nanoTime - now
      println(s"Num partitions for aggregateTypes: ${aggregateTypes.partitions.length} ${elapsed / 1000000000.0}")
      println("End 1, Start 2")
      // Computing the value of S[i,j] for edges
      now = System.nanoTime
      val newGraph = Graph(
        rankGraph.vertices.leftJoin(aggregateTypes)(
          (a, b, c) => (b, c.getOrElse(Array[Double]()))
          ),
        rankGraph.edges
      ).mapTriplets(triplet => {
        val e = EdgeProperties()
        val srcSum = triplet.srcAttr._2(triplet.dstAttr._1.vType.id)
        val dstSum = triplet.dstAttr._2(triplet.srcAttr._1.vType.id)
        val r = triplet.attr.R
        e.S = (1.0/math.sqrt(srcSum))*r*(1.0/math.sqrt(dstSum))
        e
      }).cache()//.repartition(rankGraph.edges.partitions.length).cache()
      newGraph.edges.foreachPartition(x => {})
      elapsed = System.nanoTime - now
      println(s"Num partitions for newGraph: ${newGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
      println("End 2, Start 3")
      // Computing the new rank distribution for each class
      now = System.nanoTime
      val rankUpdates = newGraph.aggregateMessages[Array[Double]](
        ctx=>{
          val srcType = ctx.srcAttr._1.vType.id
          val dstType = ctx.dstAttr._1.vType.id
          ctx.sendToDst(ctx.srcAttr._1.rankDistribution.map(x => lambda(srcType)(dstType)*ctx.attr.S*x).array)
          ctx.sendToSrc(ctx.dstAttr._1.rankDistribution.map(x => lambda(dstType)(srcType)*ctx.attr.S*x).array)
        },
        (a1, a2) => a1.zip(a2).map(a => a._1 + a._2),
        TripletFields.All
      ).cache()
      rankUpdates.foreachPartition(x => {})
      elapsed = System.nanoTime - now
      println(s"Num partitions for newGraph: ${rankUpdates.partitions.length} ${elapsed / 1000000000.0}")
      println("End 3, Start 4")
      //prevRankGraph = rankGraph
      // Including the initial rank distribution in the current rank distribution
      now = System.nanoTime
      rankGraph = newGraph.mapVertices((vid, vattr) => vattr._1).joinVertices(rankUpdates)(
        (vid, vattr, u) => {
          val v = vattr.copy()
          v.rankDistribution = vattr.rankDistribution.clone()
          v.initialRankDistribution = vattr.initialRankDistribution.clone()
          v.rankDistribution = v.rankDistribution.zip(v.initialRankDistribution).map(x => x._1 + x._2*alpha(v.vType.id))
          for(i <- 0 to 3){
            v.rankDistribution(i) = (u(i) + alpha(v.vType.id)*v.initialRankDistribution(i))/rankDenominator(v.vType.id)
          }
          v
      }).cache()//.repartition(rankGraph.edges.partitions.length).cache()
      rankGraph.edges.foreachPartition(x => {})
      elapsed = System.nanoTime - now
      println(s"Num partitions for newGraph: ${rankGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
      println("End 4")
      //prevRankGraph.unpersist(false)
      //rankUpdates.unpersist(false)
      //newGraph.unpersist(false)
      //aggregateTypes.unpersist(false)
      iteration += 1
    }
    rankGraph
  }

  /**
   * Run a dynamic version of PageRank returning a graph with vertex attributes containing the
   * PageRank and edge attributes containing the normalized edge weight.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute PageRank
   * @param tol the tolerance allowed at convergence (smaller => more accurate).
   * @param resetProb the random reset probability (alpha)
   *
   * @return the graph containing with each vertex containing the PageRank and each edge
   *         containing the normalized weight.
   */
  def runUntilConvergence[VD: ClassTag, ED: ClassTag](
                                                       graph: Graph[VD, ED], tol: Double, resetProb: Double = 0.15): Graph[Double, Double] =
  {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => {
        1.0 / e.srcAttr
    } )
      // Set the vertex attributes to (initalPR, delta = 0)
      .mapVertices( (id, attr) => (0.0, 0.0) )
      .cache()

    // Define the three functions needed to implement PageRank in the GraphX
    // version of Pregel
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
      if (edge.srcAttr._2 > tol) {
        Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
      } else {
        Iterator.empty
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage = resetProb / (1.0 - resetProb)

    // Execute a dynamic version of Pregel.
    Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vertexProgram, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
  } // end of deltaPageRank
}

