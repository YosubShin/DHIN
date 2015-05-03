
import org.apache.spark.broadcast.Broadcast

import scala.reflect.ClassTag

import org.apache.spark.{SparkContext, Logging, SparkException}
import org.apache.spark.graphx._
import org.apache.spark.graphx.TripletFields
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object IterativeNetworkConstruction extends Logging {

  def run(sc: SparkContext, graph: Graph[VertexProperties, EdgeProperties], numIterations: Int, lambda: Array[Array[Double]], alpha: Array[Double], numTypes: Int, numClasses: Int)
  : Graph[VertexProperties, EdgeProperties] = {
    val typeIndices: Array[VertexRDD[VertexProperties]] = Array.ofDim(numTypes)
    for (i <- 0 until typeIndices.length) {
      typeIndices(i) = graph.vertices.filter(v => v._2.vType.id == i)
    }

    var rankGraph = graph
    var iteration = 0
    val broadcast_lambda = sc.broadcast(lambda)
    val broadcast_alpha = sc.broadcast(alpha)

    rankGraph.checkpoint()
    rankGraph.edges.foreachPartition(x => {})

    while (iteration < numIterations) {
      // Rank objects in each class
      rankGraph = authorityRank(rankGraph, broadcast_lambda, broadcast_alpha)

      // Adjusting the Network by changing link weights
      rankGraph = adjustNetwork(sc, rankGraph, typeIndices, iteration, numTypes, numClasses)

      iteration += 1
    }
    rankGraph
  }

  def authorityRank(rankGraph: Graph[VertexProperties, EdgeProperties], broadcast_lambda: Broadcast[Array[Array[Double]]], broadcast_alpha: Broadcast[Array[Double]]) = {
    val rankDenominator = broadcast_lambda.value.zip(broadcast_alpha.value).map(x => x._1.sum + x._2)
    rankGraph.cache()
    //val now1 = System.nanoTime
    println("Start 1")
    val now = System.nanoTime
    // Vertices collecting R[i,j] values from neighbors via aggregateMessages
    val aggregateTypes: VertexRDD[Array[Array[Double]]] = rankGraph.aggregateMessages[Array[Array[Double]]](
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
    //var elapsed = System.nanoTime - now
    //println(s"Num partitions for aggregateTypes: ${aggregateTypes.partitions.length} ${elapsed / 1000000000.0}")
    //println("End 1, Start 2")
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
        e.S(k) = (1.0 / math.sqrt(srcSum(k))) * r(k) * (1.0 / math.sqrt(dstSum(k)))
      }
      e
    }).cache() //.repartition(rankGraph.edges.partitions.length).cache()

    newGraph.edges.foreachPartition(x => {})
    //elapsed = System.nanoTime - now
    //println(s"Num partitions for newGraph: ${newGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
    //println("End 2, Start 3")
    // Computing the new rank distribution for each class
    //now = System.nanoTime
    val rankUpdates = newGraph.aggregateMessages[Array[Double]](
      ctx => {
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
    //elapsed = System.nanoTime - now
    // println(s"Num partitions for newGraph: ${rankUpdates.partitions.length} ${elapsed / 1000000000.0}")

    //println("End 3, Start 4")
    // Including the initial rank distribution in the current rank distribution
    //now = System.nanoTime
    val newRankGraph = newGraph.mapVertices((vid, vattr) => vattr._1).joinVertices(rankUpdates)(
      (vid, vattr, u) => {
        val v = vattr.copy()
        val local_alpha = broadcast_alpha.value
        v.rankDistribution = vattr.rankDistribution.clone()
        v.initialRankDistribution = vattr.initialRankDistribution.clone()
        v.rankDistribution = v.rankDistribution.zip(v.initialRankDistribution).map(x => x._1 + x._2 * local_alpha(v.vType.id))
        for (i <- 0 to 3) {
          v.rankDistribution(i) = (u(i) + local_alpha(v.vType.id) * v.initialRankDistribution(i)) / rankDenominator(v.vType.id)
        }
        v
      }).cache() //.repartition(rankGraph.edges.partitions.length).cache()
    newRankGraph.edges.foreachPartition(x => {})
    val elapsed = System.nanoTime - now
    //println(s"Num partitions for newGraph: ${rankGraph.edges.partitions.length} ${elapsed / 1000000000.0}")
    //println("End 4")
    println(s"Iteration time: ${elapsed / 1000000000.0}" )
    newRankGraph
  }

  def adjustNetwork(sc: SparkContext, graph: Graph[VertexProperties, EdgeProperties], typeIndices:Array[VertexRDD[VertexProperties]], iteration: PartitionID, numTypes: Int, numClasses: Int): Graph[VertexProperties, EdgeProperties] = {
    // Calculate max P(x_{ip} | \Chi_{i}, k)
    // Row: Type, Column: Class
    val maxRankForClassesForTypes = Array.ofDim[Double](numTypes, numClasses)
    for (i <- 0 until numTypes) {
      val maxRankForClasses = graph.vertices.innerJoin(typeIndices(i))((vId, vAttr, _) => {
        vAttr
      }).aggregate(Array.ofDim[Double](numTypes))((arr, v) => {
        arr.zip(v._2.rankDistribution).map(x => Math.max(x._1, x._2))
      }, (arr1, arr2) => {
        arr1.zip(arr2).map(x => Math.max(x._1, x._2))
      })
      maxRankForClassesForTypes(i) = maxRankForClasses
    }
    val broadcastMaxRankForClassesForTypes = sc.broadcast(maxRankForClassesForTypes)

    val r = 1.0 / Math.pow(2, iteration)
    graph.mapTriplets(triplet => {
      // Calculate R_{ij,pq}(k) by using P(x_{ip} | \Chi_{i}, k)
      val edgeProperties = triplet.attr
      val old_r_ijpq = edgeProperties.R
      val src = triplet.srcAttr
      val dst = triplet.dstAttr

      val maxArr = broadcastMaxRankForClassesForTypes.value

      val resultEdgeProperties = EdgeProperties()
      for (k <- 0 until numClasses) {
        var geometricMean = 0.0
        if (maxArr(src.vType.id)(k) != 0 && maxArr(dst.vType.id)(k) != 0) {
          geometricMean = Math.sqrt(src.rankDistribution(k) / maxArr(src.vType.id)(k) * dst.rankDistribution(k) / maxArr(dst.vType.id)(k))
        }
        resultEdgeProperties.R(k) = old_r_ijpq(k) * (r + geometricMean)
        resultEdgeProperties.S(k) = edgeProperties.S(k)
      }

      resultEdgeProperties
    })

  }
}