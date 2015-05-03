
import org.apache.spark.graphx.{TripletFields, _}
import org.apache.spark.{Logging, SparkContext}

object EM extends Logging {

  def run(sc: SparkContext, ranks: Graph[VertexProperties, EdgeProperties], numIter: Int, initialRelativeSizesOfClassesForTypes: Array[Array[Double]])
  : Array[Array[Double]] = {
    var iteration = 0
    // Row: Type \Chi_{i}, Column: Class k 
    val relativeSizesOfClassesForTypes = initialRelativeSizesOfClassesForTypes
    val numTypes = initialRelativeSizesOfClassesForTypes.length
    val numClasses = initialRelativeSizesOfClassesForTypes(0).length

    println(s"Initially, the relative size of classes in types:")
    println(relativeSizesOfClassesForTypes.deep.mkString("\n"))

    // Get number of objects for each object type i
    val numObjectsForTypes = ranks.vertices.map(v => {
      val resultArray = Array.ofDim[Double](numTypes)
      val vTypeNum = v._2.vType.id
      resultArray(vTypeNum) = 1

      resultArray
    }).reduce((v1, v2) => {
      v1.zip(v2).map(x => x._1 + x._2)
    })

    // EM Algorithm
    while (iteration < numIter) {
      // E-step: Update probability of object of type \Chi_{i} belonging to class k using:
      // P(k | x_{ip}, \Chi_{i}) = P(x_{ip} | \Chi_{i}, k) * P(k | \Chi_{i})
      val probInClassesForObjs = ranks.vertices.map(v => {
        val vAttr = v._2
        val vTypeNum = vAttr.vType.id
        val relativeSizeOfClasses = initialRelativeSizesOfClassesForTypes(vTypeNum)
        val probInClasses =  vAttr.rankDistribution.zip(relativeSizeOfClasses).map(x => x._1 * x._2)

        // For convenient reduce, it returns array of size of number of types, where each
        val probInClasses2dArray = Array.ofDim[Double](numTypes, numClasses)
        probInClasses2dArray(vTypeNum) = probInClasses

        println(s"Obj #${v._1}, ProbInClasses:${probInClasses.mkString(" ")}")

        probInClasses2dArray
      })

      // M-step: Update relative sizes of classes for types
      // P(k | \Chi_{i}) = \sum_{p=1}^{n_{i}} P(k | x_{ip}, \Chi_{i}) / n_{i}
      // where n_{i}: number of objs in type i

      // M-1: Sum probabilities of each object belonging to class k for all classes
      val sumProbInClassesForObjs = probInClassesForObjs.reduce((arr1, arr2) => {
        arr1.zip(arr2).map(x => {
          val row1 = x._1
          val row2 = x._2

          row1.zip(row2).map(y => y._1 + y._2)
        })
      })

      // M-2: normalize probabilities of each object belonging to class k for all classes by dividing it to n_{i}
      // And update relative size of classes for types
      for (i <- 0 until numTypes) {
        val row = sumProbInClassesForObjs(i)
        val numObjInType = numObjectsForTypes(i)
        relativeSizesOfClassesForTypes(i) = row.map(x => x / numObjInType)
      }

      println(s"After $iteration 'th iteration of EM, the relative size of classes in types:")
      println(relativeSizesOfClassesForTypes.deep.mkString("\n"))

      iteration += 1
    }
    relativeSizesOfClassesForTypes
  }
}