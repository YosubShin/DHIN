import org.apache.spark.graphx.{EdgeDirection, TripletFields, VertexRDD, Graph}
import org.apache.spark.{SparkContext, Logging}

import scala.collection.mutable.ListBuffer

/**
 * Created by lmlesli2 on 3/20/15.
 */

object VType extends Enumeration {
  type VType = Value
  val WEBSITE, FACT, OBJECT = Value
}

case class OProp(val vType: VType.VType,
                 var value: Double,
                 val property: Any = null,
                 val msg: Any = null) extends VProperty

// !!!!! edges must be from source to fact, and from fact to object !!!!!
object TruthFinder extends Logging {
  // We will define our own versions of imp for our datasets
  def impBooks(o1: OProp, o2: OProp): Double = {
    (o1.property.asInstanceOf[Array[String]]
      .intersect(o2.property.asInstanceOf[Array[String]])
      ).size/o1.property.asInstanceOf[Array[String]].size - 0.5
  }

  // some sources, many facts, some objects
  def runSingleFact(sc: SparkContext, graph: Graph[OProp, Double], numIter: Int) : Graph[OProp, Double] = {
    // edge between Facts and Websites, and Facts and Objects




    var scoreGraph: Graph[OProp, Double] = graph
    //var g = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => (vdata, deg.getOrElse(0)) }
    //g.triplets.collect.foreach(x => println(x.srcAttr._1.vType + " " + x.srcAttr._1.value + " " + x.srcAttr._1.property.asInstanceOf[String] + " " + x.dstAttr._1.property.asInstanceOf[String] +  " " + x.srcAttr._2))

      // Set the weight on the edges based on the degree

      .mapTriplets(e => {
      //if(e.srcAttr._1.vType == VType.WEBSITE){
        1.0 / e.srcAttr._2 // from source to fact
      //} else {
      //  0.0 // from fact to object
      //}
    }, TripletFields.Src ).mapVertices((id, attr) => {
      /*if(attr._1.vType == VType.WEBSITE){
        OProp(attr._1.vType, 0.9, attr._1.property)
      }else{
        OProp(attr._1.vType, attr._1.value, attr._1.property)
      }*/
      attr._1
    })

    // send messages from sources to facts and vice versa
    var iteration = 0
    var prevScoreGraph: Graph[OProp, Double] = null
    var activeWebsites: VertexRDD[Double] = graph.vertices.filter(v => v._2.vType == VType.WEBSITE).mapValues(x => 0.0)
    var activeFacts: VertexRDD[Double] = null
    // !!!!!! START TIMER HERE !!!!!!
    // CHECKPOINT BEFORE STARTING BECUZ LINEAGE HUGE
    while(iteration < numIter) {
      scoreGraph.cache()
      // send messages from sources to facts and vice versa
      /**
       * -----------------------------------------------------------
       * --------- Confidence-Trustworthiness propagation ----------
       * -----------------------------------------------------------
       */
      // Propagate from sources to facts using Equation (3).
      // We only want to send from websites, so we pass them in as the
      // active vertex set.
      // Returns rdd of facts which we use as the active vertex set in
      // the next mapReduceTriplets
      activeFacts = scoreGraph.mapReduceTriplets[Double](
        ctx => {
          if(ctx.srcAttr.vType == VType.WEBSITE){
            // will always be here due to active vertex set
            Iterator((ctx.dstId, -math.log(1.0 - ctx.srcAttr.value)))
          }else{
            Iterator.empty
          }
        },
        (v1, v2) => v1 + v2,
        Some((activeWebsites, EdgeDirection.Out))
      ).cache()
      prevScoreGraph = scoreGraph
      // Join here -- need to update facts with new message sum
      // using Equations (5) and (7)
      scoreGraph = scoreGraph.joinVertices(activeFacts){
        (id, attr, msgSum) => OProp(attr.vType, 1.0 - math.exp(-1.0*msgSum))
      }
      prevScoreGraph.vertices.unpersist(blocking = false)
      prevScoreGraph.edges.unpersist(blocking = false)
      // propagate from facts to sources
      activeWebsites = scoreGraph.mapReduceTriplets[Double](
        ctx => {
          if(ctx.srcAttr.vType == VType.FACT){
            // will always be here due to active vertex set
            Iterator((ctx.srcId, ctx.srcAttr.value * ctx.attr))
          }else{
            Iterator.empty
          }
        },
        (v1, v2) => v1 + v2,
        Some((activeFacts, EdgeDirection.In))
      ).cache()
      prevScoreGraph = scoreGraph
      // join here -- need to update websites with new trustworthiness
      scoreGraph = scoreGraph.joinVertices(activeWebsites){
        (id, attr, msgSum) => OProp(attr.vType, msgSum, attr.property)
      }
      scoreGraph.edges.foreachPartition(x => {})
      prevScoreGraph.vertices.unpersist(blocking = false)
      prevScoreGraph.edges.unpersist(blocking = false)
      iteration += 1
    }
    scoreGraph

  }

  def runMultiFact(sc: SparkContext, graph: Graph[OProp, Double], numIter: Int, gamma: Double, rho: Double) : Graph[OProp, Double] = {
    // edge between Facts and Websites, and Facts and Objects
    var scoreGraph: Graph[OProp, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => (vdata, deg.getOrElse(0)) }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr._2, TripletFields.Src )
      .mapVertices((id, attr) => OProp(attr._1.vType, attr._1.value, attr._1.property))
    // send messages from sources to facts and vice versa
    var iteration = 0
    while(iteration < numIter) {
      scoreGraph.cache()
      /**
       * -----------------------------------------------------------
       * --------- Confidence-Trustworthiness propagation ----------
       * -----------------------------------------------------------
       */
      // send messages from sources to facts and vice versa
      val scoreUpdates: VertexRDD[Double] = scoreGraph.aggregateMessages[Double](
        ctx => {
          if ((ctx.srcAttr.vType == VType.FACT || ctx.srcAttr.vType == VType.WEBSITE)
            && (ctx.dstAttr.vType == VType.FACT || ctx.dstAttr.vType == VType.WEBSITE)) {
            ctx.sendToDst(ctx.srcAttr.value * ctx.attr)
            ctx.sendToSrc(ctx.dstAttr.value * ctx.attr)
          }
        },
        (v1, v2) => v1 + v2,
        TripletFields.All
      )
      //rankUpdates.foreachPartition(x => {})
      scoreGraph = scoreGraph.joinVertices(scoreUpdates) {
        (id, attr, msgSum) => {
          // msgSum will be null for Objects only
          // which is OK
          OProp(attr.vType, msgSum, attr.property)
        }
      }
      /**
       * -----------------------------------------------------------
       * -------------- Fact Confidence calculation-----------------
       * -----------------------------------------------------------
       * This can be made more efficient somehow. At the moment, we
       * send each fact to linked objects, then send the aggregated
       * facts back to each linked fact. We then calculate the sum
       * in Equation 6, followed by Equation 8.
       * -----------------------------------------------------------
       */

      // Send facts to each object.
      val aggFacts = scoreGraph.aggregateMessages[Array[OProp]](
        ctx => {
          if (ctx.dstAttr.vType == VType.OBJECT && ctx.srcAttr.vType == VType.FACT){
            ctx.sendToDst(Array(ctx.srcAttr))
          }else if(ctx.dstAttr.vType == VType.FACT || ctx.srcAttr.vType == VType.OBJECT){
            ctx.sendToSrc(Array(ctx.dstAttr))
          }
        },
        (v1, v2) => {
          v1 ++ v2
        },
        TripletFields.All
      ).cache()
      // Aggregate facts at each object.
      scoreGraph = scoreGraph.joinVertices(aggFacts){
        (id, attr, msgSum) => {
          // msgSum will be null for sources and facts
          if(msgSum == null){
            OProp(attr.vType, attr.value, attr.property)
          }else{
            // for objects, set it
            OProp(attr.vType, attr.value, attr.property, msgSum)
          }
        }
      }
      // Send aggregated facts back to each fact. This is inefficient,
      // as there are few sources and we are likely iterating through
      // roughly half the total number of edges.
      val aggSigma = scoreGraph.aggregateMessages[Array[OProp]](
        ctx => {
          if (ctx.dstAttr.vType == VType.OBJECT && ctx.srcAttr.vType == VType.FACT){
            ctx.sendToSrc(ctx.dstAttr.msg.asInstanceOf[Array[OProp]])
          }else if(ctx.dstAttr.vType == VType.FACT || ctx.srcAttr.vType == VType.OBJECT){
            ctx.sendToDst(ctx.srcAttr.msg.asInstanceOf[Array[OProp]])
          }
        },
        (v1, v2) => {
          v1 ++ v2
        },
        TripletFields.All
      ).cache()
      // Aggregate facts at each fact and calculate the confidence of each fact.
      // This is inefficient, especially if the number of sources is large,
      // as we must iterate through every vertex.
      scoreGraph = scoreGraph.joinVertices(aggFacts){
        (id, attr, msgSum) => {
          // msgSum will be null for sources and objects
          if(attr.vType == VType.FACT){
            val sum: Double = msgSum.foldLeft[Double](0.0)((d, prop) => {
              d + prop.value*impBooks(attr, prop)
            })
            val sigmaStar = attr.value + rho*(sum - attr.value) // Equation 6
            val s = 1.0 / (1.0 - math.exp(-1.0*gamma * sigmaStar)) // Equation 8
            OProp(attr.vType, s, attr.property) // facts
          }
          else{
            OProp(attr.vType, attr.value)
          }
        }
      }

      scoreGraph.edges.foreachPartition(x => {})
      iteration += 1
    }
    scoreGraph
  }

}
