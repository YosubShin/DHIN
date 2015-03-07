
object ResearchArea extends Enumeration {
  type ResearchArea = Value
  val DATABASES, DATA_MINING, AIML, IR, NONE = Value
}

object VertexType extends Enumeration {
  type VertexType = Value
  val PAPER, VENUE, AUTHOR, TERM = Value
}

class VProperty()

case class VertexProperties(k: Int, vType: VertexType.VertexType, attribute: String, l: ResearchArea.ResearchArea) extends VProperty{
  var label: ResearchArea.ResearchArea = l
  val rankDistribution: Array[Double] = Array.ofDim(k)
  val prevRankDistribution: Array[Double] = Array.ofDim(k)
  var subnetworkId: Int = -1
}
