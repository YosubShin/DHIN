
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

import scala.util.Random




class RankClass(sc: SparkContext, k: Int, numPartitions: Int) {


  def run(): Unit = {
    val graph = GenerateGraph.generate(sc, k, numPartitions)
    val countArray = graph.vertices.aggregate(Array.ofDim[Int](k, 4))((a, b) => {
      a(b._2.label.id)(b._2.vType.id) += 1
      a
    }, (a1, a2) => {
      for(i <- 0 to k-1; j <- 0 to 3){
        a1(i)(j) += a2(i)(j)
      }
      a1
    })

    val oldVerts = graph.vertices

    var newVerts = graph.vertices.map(v => {
      if(v._2.label == ResearchArea.NONE) {
        v._2.label = Util.intToRA(Random.nextInt(k))
        v._2.rankDistribution.transform(x => 0.0)
        v._2.prevRankDistribution.transform(x => 0.0)
      }else{
        v._2.rankDistribution.transform(x => 1.0/countArray(v._2.label.id)(v._2.vType.id))
        v._2.prevRankDistribution.transform(x => 1.0/countArray(v._2.label.id)(v._2.vType.id))
      }
      v
    })

    oldVerts.unpersist(false)




  }


}
