class EProperty() extends Serializable

case class EdgeProperties() extends VProperty {
  val R = Array.ofDim[Double](4).transform(x => 1.0).array
  val S = Array.ofDim[Double](4)
}