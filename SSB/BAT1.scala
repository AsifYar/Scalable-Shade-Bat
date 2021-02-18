package ShadeBatIsland80

import scala.util.Random

case class BAT1 ( posVals : Array[Double] , velVals : Array[Double] , f:  Array[Double] =>  Double ,
                   fitnessVal: Double , LR1 : Double  , PR1 : Double
                ) extends Serializable  {

  var position      : Array[Double]          =     posVals 
  var velocity      : Array[Double]          =     velVals 
  var fitness       : Double                  =     fitnessVal
  var PulseRate: Double = PR1
  var LoudnessRate: Double = LR1
  val rnd = new Random()


  def move  (  local_Best : Array  [  Double  ]  ,  size : Int ,Fmin : Double , Fmax : Double , Bandwidth : Double,mini:Int , maxi : Int ,
               arr : Array[Array[Double]] ,  sumLoud : Double )  : (  Array  [  Double  ]  ,  Double ,Int   )  = {

    val freq = Fmin + (     Fmax - Fmin      )  *  math.random
    var counter = 0

    velocity      =   ES ( position , local_Best , velocity  , freq , mini,maxi )
    var newPos    =   ES1 ( position , velocity   )
    if (math.random > PulseRate) {
      newPos = eShadeLsLs(arr , local_Best , local_Best.length ,mini ,maxi ,Bandwidth, sumLoud )  
    counter += local_Best.length
    }
    val nFitness = f  (  newPos  )
    counter += 1

    ( newPos   ,  nFitness  , counter )

  }

  def eShadeLsLs( arr : Array[Array[Double]]  ,bestSol : Array[Double]  , iterationss : Int ,mini:Int , maxi : Int , ban : Double ,
                  sumLoud : Double ) : Array[Double] = {

    def loop (best:Array[Double] , fit : Double , iter : Int , indxi : Int):Array[Double] = {
      if( iter == iterationss)
        best
      else {
        val k0 = rnd.nextInt(arr.length - 1)
        val n = rnd.nextInt(bestSol.length - 1)
        val newSol =   newSol.map( x => keepWithinRange1( (x + ban * sumLoud) * (newSol(n) -  arr(k0)(n) ))
          , mini , maxi, x )
        val newFit = f (newSol)
        if(newFit <= fit)
          loop(newSol,newFit,iter+1 ,  indxi + 1  )
        else
          loop(best ,fit, iter + 1 , indxi + 1   )
      }
    }
    loop(bestSol , f (bestSol) ,1 , 0 )
  }
  def keepWithinRange2(x: Int ) : Int = if (x >= 1000) 0 else x

  def keepWithinRange1(x: Double, min: Int, max: Int , bestIndAtJ : Double) : Double = {

    val res =if (x > max )
      (max +bestIndAtJ ) / 2.0
    else if
    ( x < min)
      (min + bestIndAtJ) / 2.0
    else
      x
    res
  }
  def keepWithinRange(x: Double, min: Int, max: Int) : Double = math.max(min, math.min(max, x))
  def ES1(arr: Array[Double], arr1: Array[Double]): Array[Double] = {
    val newArr = new Array[Double](arr.length)
    var i = 0
    while (i < arr.length) {
      newArr(i) = arr(i) + arr1(i)
      i += 1
    }
    newArr
  }
  def ES(pos: Array[Double], best :  Array[Double] , vel : Array[Double] , frequ : Double  ,mini:Int , maxi : Int )  : Array[Double] = {
    val length = pos.length
    val newVel = Array.fill(length)( math.random )

    var i = 0
    while (i < length) {
      newVel(i) =  keepWithinRange(  vel(i) + ( (    pos(i)   -  best(i))    * frequ ) , mini ,maxi )
      i += 1
    }
    newVel
  }



  def LocalSearch(arr: Array[Double], b: Double ,mini:Int , maxi : Int ): Array[Double] = {
    val newArr = new Array[Double](arr.length)
    val rnd = new Random()
    var i = 0
    while (i < arr.length) {
      newArr(i) =  keepWithinRange(  arr(i) + b * rnd.nextGaussian() , mini, maxi)
      i += 1
    }
    newArr
  }


}
