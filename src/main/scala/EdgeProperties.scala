class EProperty() extends Serializable

case class EdgeProperties(k: Int) extends VProperty{
  val weights: Array[Double] = Array.ofDim(k).map(x => 1.0)
  val prevWeights: Array[Double] = Array.ofDim(k).map(x => 1.0)
}