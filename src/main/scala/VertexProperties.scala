
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
  val initialRankDistribution: Array[Double] = Array.ofDim[Double](k).transform(x => 0.0).array


  //2-D array: rows object type, column objects belonging to object type
  //1-D array: columns all neighbor
  val RSum: Array[Double] = Array.ofDim[Double](4).transform(x => 0.0).array

  var subnetworkId: Int = -1
}
