
object ResearchArea extends Enumeration {
  type ResearchArea = Value
  val DATABASES, DATA_MINING, AIML, IR, NONE = Value
}

object VertexType extends Enumeration {
  type VertexType = Value
  val PAPER, VENUE, AUTHOR, TERM = Value
}




class VProperty() extends Serializable

case class VertexProperties(k: Int, vType: VertexType.VertexType, attribute: String, l: ResearchArea.ResearchArea) extends VProperty{
  var label: ResearchArea.ResearchArea = l
  var rankDistribution: Array[Double] = Array.ofDim[Double](k).transform(x => 0.0).array
  var initialRankDistribution: Array[Double] = Array.ofDim[Double](k).transform(x => 0.0).array


  //2-D array: rows object type, column objects belonging to object type
  //1-D array: columns all neighbor
//  var RSum: Array[Double] = Array.ofDim[Double](4).transform(x => 0.0).array
//  var sumRij: Array[Array[Double]] = null

  var subnetworkId: Int = -1



  def createCopy(): VertexProperties = {
    val v = new VertexProperties(k, vType, attribute, l)
    v.label = label
    v.rankDistribution = rankDistribution.clone()
    v.initialRankDistribution = initialRankDistribution.clone()
//    v.RSum = RSum.clone()
    v.subnetworkId = subnetworkId
    v
  }


}
