
import org.apache.spark.graphx.{TripletFields, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}

object EM extends Logging {

  def run(sc: SparkContext, ranks: Graph[VertexProperties, EdgeProperties], numIter: Int, numTypes: Int, numClasses: Int)
  : (VertexRDD[Array[Array[Double]]], Array[Array[Double]]) = {
    var iteration = 0
    // i-th Row: Type \Chi_{i}, j-th Column: Class j
    val initialRelativeSizesOfClassesForTypes = Array.ofDim[Double](numTypes, numClasses).transform(x => x.transform(y => 1.0 / numClasses).array).array
    val relativeSizesOfClassesForTypes = initialRelativeSizesOfClassesForTypes

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

    var probInClassesForObjs: VertexRDD[Array[Array[Double]]] = null

    // EM Algorithm
    while (iteration < numIter) {
      val now = System.nanoTime

      // E-step: Update probability of object of type \Chi_{i} belonging to class k using:
      // P(k | x_{ip}, \Chi_{i}) = P(x_{ip} | \Chi_{i}, k) * P(k | \Chi_{i})
      println(s"Starting Expectation Step #$iteration")

      probInClassesForObjs = VertexRDD(ranks.vertices.map(v => {
        val vAttr = v._2
        val vTypeNum = vAttr.vType.id
        val relativeSizeOfClasses = relativeSizesOfClassesForTypes(vTypeNum)
        var probInClasses =  vAttr.rankDistribution.zip(relativeSizeOfClasses).map(x => x._1 * x._2)
        
        // sum of probInClasses should add up to 1
        val sumProbInClasses = probInClasses.sum
        probInClasses = probInClasses.map(x => x / sumProbInClasses)

        // For convenient reduce, it returns array of size of number of types, where each
        val probInClasses2dArray = Array.ofDim[Double](numTypes, numClasses)
        probInClasses2dArray(vTypeNum) = probInClasses

        println(s"Obj #${v._1}, ProbInClasses:${probInClasses.mkString(" ")}")

        (v._1, probInClasses2dArray)
      }))

      // M-step: Update relative sizes of classes for types
      // P(k | \Chi_{i}) = \sum_{p=1}^{n_{i}} P(k | x_{ip}, \Chi_{i}) / n_{i}
      // where n_{i}: number of objs in type i
      println(s"Starting Maximization Step #$iteration")

      // M-1: Sum probabilities of each object belonging to class k for all classes
      val sumProbInClassesForObjs = probInClassesForObjs.aggregate(Array.ofDim[Double](numTypes, numClasses))((arr1, v) => {
        val arr2 = v._2
        arr1.zip(arr2).map(x => {
          val row1 = x._1
          val row2 = x._2

          row1.zip(row2).map(y => y._1 + y._2)
        })
      }, (arr1, arr2) => {
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

      val elapsed = System.nanoTime - now
      println(s"Iteration time: ${elapsed / 1000000000.0}" )
    }
    (probInClassesForObjs, relativeSizesOfClassesForTypes)
  }
}