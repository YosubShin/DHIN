
object Util {
  var timestamp: Int = 0

  /*














   */
  def intToRA(i: Int): ResearchArea.ResearchArea ={
    if(i < 4){
      ResearchArea(i)
    }else{
      ResearchArea.NONE
    }
  }

}
