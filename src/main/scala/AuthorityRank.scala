
import scala.reflect.ClassTag

import org.apache.spark.{SparkContext, Logging, SparkException}
import org.apache.spark.graphx._
import org.apache.spark.graphx.TripletFields
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object AuthorityRank extends Logging {

  def run(sc: SparkContext, graph: Graph[VertexProperties, EdgeProperties], numIter: Int, lambda: Array[Array[Double]], alpha: Array[Double])
  : Graph[VertexProperties, EdgeProperties] = {
    val rankDenominator = lambda.zip(alpha).map(x => x._1.sum + x._2)
    var rankGraph = graph
    var iteration = 1
    var prevRankGraph: Graph[VertexProperties, EdgeProperties] = null

    graph.checkpoint()

    val r: Array[Double] = Array.ofDim[Double](numIter)
    for (i <- 0 to numIter - 1) {
      r(i) = 1.0 / math.pow(2, i)
    }
    val broadcast_r = sc.broadcast(r)

    val broadcast_lambda = sc.broadcast(lambda)
    val broadcast_alpha = sc.broadcast(alpha)

    while (iteration < numIter) {
      rankGraph.cache()

      println("Start 1")
      var now = System.nanoTime
      // Vertices collecting R[i,j] values from neighbors via aggregateMessages
      val aggregateTypes:VertexRDD[Array[Array[Double]]] = rankGraph.aggregateMessages[Array[Array[Double]]](
        ctx => {
          ctx.sendToDst({
            val arr = Array.ofDim[Double](4, 4)
            arr(ctx.srcAttr.vType.id) = ctx.attr.R
            arr
          })
          ctx.sendToSrc({
            val arr = Array.ofDim[Double](4, 4)
            arr(ctx.dstAttr.vType.id) = ctx.attr.R
            arr
          })
        },
        (a1, a2) => {
          val a = Array.ofDim[Double](4, 4)
          for (i <- 0 to 3; j <- 0 to 3) {
            a(i)(j) = a1(i)(j) + a2(i)(j)
          }
          a
        },
        TripletFields.All
      ).cache()
      aggregateTypes.foreachPartition(x => {})
      var elapsed = System.nanoTime - now
      println(s"Num partitions for aggregateTypes: ${aggregateTypes.partitions.length} ${elapsed / 1000000000.0}")
      println("End 1, Start 2")
      // Computing the value of S[i,j] for edges

      // --- VERSION 1 ----
      // need to optimize this
      val newGraph = Graph(
        rankGraph.vertices.leftJoin(aggregateTypes)(
          (a, b, c) => (b, c.getOrElse(Array[Array[Double]]()))
        ),
        rankGraph.edges
      ).mapTriplets(triplet => {
        val e = EdgeProperties()
        val srcSum = triplet.srcAttr._2(triplet.dstAttr._1.vType.id)
        val dstSum = triplet.dstAttr._2(triplet.srcAttr._1.vType.id)
        val r = triplet.attr.R
        for (k <- 0 to 3) {
          e.S(k) = (1.0/math.sqrt(srcSum(k)))*r(k)*(1.0/math.sqrt(dstSum(k)))
        }
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
          val local_lambda = broadcast_lambda.value
          val dstMsg = Array.ofDim[Double](4)
          val srcMsg = Array.ofDim[Double](4)
          for (k <- 0 to 3) {
            dstMsg(k) = local_lambda(srcType)(dstType) * ctx.attr.S(k) * ctx.srcAttr._1.rankDistribution(k)
            srcMsg(k) = local_lambda(dstType)(srcType) * ctx.attr.S(k) * ctx.dstAttr._1.rankDistribution(k)
          }
          ctx.sendToDst(dstMsg)
          ctx.sendToSrc(srcMsg)
          //          ctx.sendToDst(ctx.srcAttr._1.rankDistribution.map(x => local_lambda(srcType)(dstType)*ctx.attr.S*x).array)
          //          ctx.sendToSrc(ctx.dstAttr._1.rankDistribution.map(x => local_lambda(dstType)(srcType)*ctx.attr.S*x).array)
        },
        (a1, a2) => a1.zip(a2).map(a => a._1 + a._2),
        TripletFields.All
      ).cache()

      rankUpdates.foreachPartition(x => {})
      elapsed = System.nanoTime - now
      println(s"Num partitions for newGraph: ${rankUpdates.partitions.length} ${elapsed / 1000000000.0}")

      println("End 3, Start 4")
      // Including the initial rank distribution in the current rank distribution
      now = System.nanoTime
      rankGraph = newGraph.mapVertices((vid, vattr) => vattr._1).joinVertices(rankUpdates)(
        (vid, vattr, u) => {
          val v = vattr.copy()
          val local_alpha = broadcast_alpha.value
          v.rankDistribution = vattr.rankDistribution.clone()
          v.initialRankDistribution = vattr.initialRankDistribution.clone()
          v.rankDistribution = v.rankDistribution.zip(v.initialRankDistribution).map(x => x._1 + x._2*local_alpha(v.vType.id))
          for(i <- 0 to 3){
            v.rankDistribution(i) = (u(i) + local_alpha(v.vType.id)*v.initialRankDistribution(i))/rankDenominator(v.vType.id)
          }
          v
        }).cache()//.repartition(rankGraph.edges.partitions.length).cache()
      rankGraph.edges.foreachPartition(x => {})
      elapsed = System.nanoTime - now
      println(s"Num partitions for newGraph: ${rankGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
      println("End 4")

      /*
now = System.nanoTime
// --- VERSION 2: no new graph, instead we store Rij inside the vertex
// Rij is initially null to decrease messaging overhead, and is reset to null
// after we are done with it ----
// set Rij in each vertex so we don't need to construct another graph
rankGraph = rankGraph.joinVertices(aggregateTypes)((vid, vp, u) => {
  val vp_ = vp.createCopy()
  vp_.sumRij = u.map(x => x.clone())
  vp_
}).cache()
elapsed = System.nanoTime - now
rankGraph.edges.foreachPartition(x => {})
println(s"Num partitions for newGraph: ${rankGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
println("End 2, Start 3")
//aggregateTypes.unpersist(false)
// compute S
now = System.nanoTime
rankGraph = rankGraph.mapTriplets(triplet => {
  val e = EdgeProperties()
  val srcSum = triplet.srcAttr.sumRij(triplet.dstAttr.vType.id)
  val dstSum = triplet.dstAttr.sumRij(triplet.srcAttr.vType.id)
  val r = triplet.attr.R
  for (k <- 0 to 3) {
    e.S(k) = (1.0/math.sqrt(srcSum(k)))*r(k)*(1.0/math.sqrt(dstSum(k)))
  }
  e
})
  // So that we no longer have to pass the Rij field in aggregate messages,
  // we reset it to be null here via vp.createCopy(), which copies all
  // fields except for Rij. This is the only source of extra overhead
  // for this approach and I'm pretty sure it's less than that incurred by
  // creating a brand new graph.
  .mapVertices((v, vp) => vp.createCopy()).cache()
rankGraph.edges.foreachPartition(x => {})
println(s"Num partitions for newGraph: ${rankGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
println("End 3, Start 4")
// compute the new rank distribution for each class
now = System.nanoTime
val rankUpdates = rankGraph.aggregateMessages[Array[Double]](
  ctx=>{
    val srcType = ctx.srcAttr.vType.id
    val dstType = ctx.dstAttr.vType.id
    val local_lambda = broadcast_lambda.value
    val dstMsg = Array.ofDim[Double](4)
    val srcMsg = Array.ofDim[Double](4)
    for (k <- 0 to 3) {
      dstMsg(k) = local_lambda(srcType)(dstType) * ctx.attr.S(k) * ctx.srcAttr.rankDistribution(k)
      srcMsg(k) = local_lambda(dstType)(srcType) * ctx.attr.S(k) * ctx.dstAttr.rankDistribution(k)
    }
    ctx.sendToDst(dstMsg)
    ctx.sendToSrc(srcMsg)
  },
  (a1, a2) => a1.zip(a2).map(a => a._1 + a._2),
  TripletFields.All
).cache()
rankUpdates.foreachPartition(x => {})
println(s"Num partitions for rankUpdates: ${rankUpdates.partitions.length} ${elapsed / 1000000000.0}")
println("End 4, Start 5")
// update the rank distribution for each vertex
now = System.nanoTime
rankGraph = rankGraph.joinVertices(rankUpdates)(
  (vid, vp, u) => {
    // copies all fields of attr except for Rij, which is left as null
    val vp_ = vp.createCopy()
    val local_alpha = broadcast_alpha.value
    vp_.rankDistribution = vp_.rankDistribution.zip(vp_.initialRankDistribution).map(x => x._1 + x._2*local_alpha(vp_.vType.id))
    for(i <- 0 to 3){
      vp_.rankDistribution(i) = (u(i) + local_alpha(vp_.vType.id)*vp_.initialRankDistribution(i))/rankDenominator(vp_.vType.id)
    }
    vp_
  }).cache()
//rankUpdates.unpersist(false)
rankGraph.edges.foreachPartition(x => {})
println(s"Num partitions for newGraph: ${rankGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
println("End 5")
*/


      //prevRankGraph.unpersist(false)
      //rankUpdates.unpersist(false)
      //newGraph.unpersist(false)
      //aggregateTypes.unpersist(false)

      //      // Adjusting the Network
      //      // Calculating Max rank per class and per type
      //      val maxRankDistributions = rankGraph.vertices.aggregate(Array.ofDim[Double](4,4))((u, p) => {
      //        val t = p._2.vType.id
      //        val up = u.map(_.clone)
      //        for(i <- 0 to 3){
      //          up(t)(i) = math.max(p._2.rankDistribution(i), u(t)(i))
      //        }
      //        up
      //      }, (u1, u2) => {
      //        val up = Array.ofDim[Double](4,4)
      //        for(i <- 0 to 3; j <- 0 to 3){
      //          up(i)(j) = math.max(u1(i)(j), u2(i)(j))
      //        }
      //        up
      //      }
      //      )
      //
      //      // Adjusting Ranks
      //      rankGraph.mapTriplets(triplet => {
      //        val local_r = broadcast_r.value
      //        val e = EdgeProperties()
      //
      //        for (k <- 0 to 3) {
      //          val src_rank = triplet.srcAttr.rankDistribution(k)
      //          val src_type_max_rank = maxRankDistributions(triplet.srcAttr.vType.id)(k)
      //          val dst_rank = triplet.dstAttr.rankDistribution(k)
      //          val dst_type_max_rank = maxRankDistributions(triplet.dstAttr.vType.id)(k)
      //
      //          e.R
      //        }
      //
      //      }).cache()//.repartition(rankGraph.edges.partitions.length).cache()

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

