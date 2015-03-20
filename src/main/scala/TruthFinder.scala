import org.apache.spark.graphx.{TripletFields, VertexRDD, Graph}
import org.apache.spark.{SparkContext, Logging}

import scala.collection.mutable.ListBuffer

/**
 * Created by lmlesli2 on 3/20/15.
 */

object VType extends Enumeration {
  type VType = Value
  val WEBSITE, FACT, OBJECT = Value
}

/*
trait VProp extends Serializable{
  def vType: VType.VType
  def value: Double
  def other: Any
}
*/



case class OProp(val vType: VType.VType,
                 var value: Double,
                 val property: Any = null,
                 val msg: Any = null) extends VProperty

//class TFProp extends Serializable

/*
case class WebsiteProperty(override val vType: VType.VType,
                           override var value: Double = 0.9) extends VProp
case class FactProperty(override val vType: VType.VType,
                        override var value: Double = 0.0) extends VProp
case class ObjectProperty(override val vType: VType.VType,
                          val properties: Array[String],
                          override var value: Double = 0.0) extends VProp
*/

object AuthorityRank extends Logging {

  def impBooks(o1: OProp, o2: OProp): Double = {
    (o1.property.asInstanceOf[Array[String]]
      .intersect(o2.property.asInstanceOf[Array[String]])
      ).size/o1.property.asInstanceOf[Array[String]].size - 0.5
  }

  // many sources, fewer facts, even fewer objects, so maybe we should just use RDDs?


  def runSingleFact(sc: SparkContext, graph: Graph[OProp, Double], numIter: Int, gamma: Double, rho: Double) : Graph[OProp, Double] = {
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
      // send messages from sources to facts and vice versa
      var scoreUpdates: VertexRDD[Double] = scoreGraph.aggregateMessages[Double](
        ctx => {
          if ((ctx.srcAttr.vType == VType.FACT || ctx.srcAttr.vType == VType.WEBSITE)
            && (ctx.dstAttr.vType == VType.FACT || ctx.dstAttr.vType == VType.WEBSITE)) {
            ctx.sendToDst(ctx.srcAttr.value * ctx.attr)
            ctx.sendToSrc(ctx.dstAttr.value * ctx.attr)
          }
        },
        (v1, v2) => v1 + v2,
        TripletFields.All
      ).cache()
      scoreGraph = scoreGraph.joinVertices(scoreUpdates){
        (id, attr, msgSum) => {
          //msgSum will be null for objects
          var value = 0.0
          if(attr.vType == VType.FACT){
            // set to tau (trustworthiness)
           OProp(attr.vType, -math.log(1.0 - msgSum))
          }else {
            OProp(attr.vType, msgSum, attr.property)
          }
        }
      }
      scoreGraph.edges.foreachPartition(x => {})
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
      // send messages from sources to facts and vice versa
      var scoreUpdates: VertexRDD[Double] = scoreGraph.aggregateMessages[Double](
        ctx => {
          if ((ctx.srcAttr.vType == VType.FACT || ctx.srcAttr.vType == VType.WEBSITE)
            && (ctx.dstAttr.vType == VType.FACT || ctx.dstAttr.vType == VType.WEBSITE)) {
            ctx.sendToDst(ctx.srcAttr.value * ctx.attr)
            ctx.sendToSrc(ctx.dstAttr.value * ctx.attr)
          }
        },
        (v1, v2) => v1 + v2,
        TripletFields.All
      ).cache()
      //rankUpdates.foreachPartition(x => {})
      scoreGraph = scoreGraph.joinVertices(scoreUpdates){
        (id, attr, msgSum) => {
          //msgSum will be null for objects
          OProp(attr.vType, msgSum, attr.property)
        }
      }
      // send facts to each object
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
      // aggregate facts at each object
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
      // send aggregated facts to each fact
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
      // aggregate facts at each fact
      scoreGraph = scoreGraph.joinVertices(aggFacts){
        (id, attr, msgSum) => {
          // msgSum will be null for sources and facts
          if(msgSum == null){
            OProp(attr.vType, attr.value, attr.property)
          }else{
            OProp(attr.vType, attr.value, attr.property, msgSum)
          }
        }
      }
      // calculate the confidence of each fact
      scoreGraph = scoreGraph.mapVertices((id, attr) => {
        if(attr.vType == VType.FACT){
          val sum: Double = attr.msg.asInstanceOf[Array[OProp]].foldLeft[Double](0.0)((d, prop) => {
            d + prop.value*impBooks(attr, prop)
          })
          val sigmaStar = attr.value + rho*(sum - attr.value)
          val s = 1.0 / (1.0 - math.exp(-1.0*gamma * sigmaStar))
          OProp(attr.vType, s, attr.property) // facts
        }else{
          OProp(attr.vType, attr.value) // sources and objects
        }
      })
      scoreGraph.edges.foreachPartition(x => {})
    }
    scoreGraph
  }

}
