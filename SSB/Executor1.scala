package ShadeBatIsland80

import java.io.FileWriter

import scala.math._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

class Executor1(sc: SparkContext,  Bandwidth: Double, alpha: Double, gyma: Double, InitialPulseRate: Double,
                N: Int, d: Int, partitions: Int, MinVal: Int, MaxVal: Int, f:  Array[Double]  => Double
                , mf: Int, mr: Int, T_iterations: Int , F_min : Double , F_max : Double , rateL : Double ,
                rateP : Double , Percentage:Int , NP : Int ,localSearchEvals : Int ,Path : String )
  extends Serializable {

  lazy val tmpV = Array.fill(d)(math.random)
  lazy val tmpP =  Array.fill(d)(math.random)
  val initSolution = sc.parallelize  (  List  (  (   BAT1  ( tmpP  ,tmpV, f , f(tmpP)    , rateL , rateP)  , 0  )))
  var bestSolutions = BroadcastWrapper(sc, initSolution.collect())
  val shade = new Shade(  d  ,MinVal , MaxVal ,f , mr  , N)
  val rnd = new Random()

  def execute(RDD: RDD[BAT1], c_itr: Int  , randomIsland : Int ): Array[(BAT1, Int)] = {
    val newRDD = RDD.mapPartitionsWithIndex {
      (index, Iterator) => {
        var arr: Array[BAT1] = Iterator.toArray
        if (c_itr != 0) {
          val recBD = bestSolutions.value
          val recBD1 = recBD.map(_._1) 

          val bufferedSource = Source.fromFile(Path + "data/" + index + ".txt")
          val lines : Iterator[String] = bufferedSource.getLines()

          val data : Array[BAT1] = lines.flatMap{line : String =>
            val ls = line.replaceAll("\\(", "")
              .replaceAll("\\)","")
              .replaceAll("List", "")
              .split(",")

            Seq(BAT1 ( ls.slice(0,d).map(_.toDouble), ls.slice(d, 2 * d ).map(_.toDouble), f ,
              ls(2 * d).toDouble, ls( ( 2 * d ) + 1).toDouble, ls( (2 * d )+ 2).toDouble) )
          }.toArray

          arr = data.clone()
          arr = arr.sortWith(_.fitness < _.fitness)
          arr = arr.take(arr.length - recBD.length)
          arr = recBD1 ++ arr
        }

        val Evals =  mf

        var ApplyShade : Boolean = false
        var ApplyBA : Boolean = false

        if( index % 2  == 0 )
          ApplyShade = true
        if( index % 2  == 1 )
          ApplyBA = true


        if( ApplyShade  ==  true ) {
          val positionArr: Array[Array[Double]] = arr.map(_.position)
          val (fitnesesShade, sols) = shade.improve(Evals, positionArr)

          var z = 0
          while (z < sols.length) {
            arr(z).position = sols(z)
            arr(z).fitness = fitnesesShade(z)
            z += 1
          }
        }


        if (  ApplyBA == true  ) {
          var currentEvaluations = 0
          var iteration = 0

          var localBest = arr(0).position.clone()
          var localBestFit = arr(0).fitness

          while (currentEvaluations < Evals) {
            iteration = iteration + 1
            val Bests1 = arr.sortWith(_.fitness < _.fitness).take(3)

            val Bests = Bests1.map(_.position)
            val singleBestFit = Bests1(0).fitness
            val meanBest = (Bests(0), Bests(1), Bests(2)).zipped.map(_ + _ + _).map(_ / 3.0)
            val meanBestFit = f(meanBest)
            if (singleBestFit <= localBestFit) {
              localBest = Bests(0).clone()
              localBestFit = singleBestFit
            }

            if (meanBestFit <= localBestFit) {
              localBest = meanBest
              localBestFit = meanBestFit
            }

            var i = 0
            while (i < arr.length) {

              val res = arr(i).move(localBest, arr.length, F_min, F_max, Bandwidth, MinVal, MaxVal ,arr.map(_.position) , arr.map(_.LoudnessRate).sum /arr.length )
              currentEvaluations += (NP * res._3)


              if (math.random < arr(i).LoudnessRate && res._2 <= arr(i).fitness) {

                arr(i) = BAT1(res._1, arr(i).velocity, f, res._2, arr(i).LoudnessRate * alpha, InitialPulseRate * (1 - pow(E, (-gyma * iteration))))

              }

              if (res._2 <= localBestFit) {
                localBest = res._1
                localBestFit = res._2
              }
              if(currentEvaluations >= Evals ){
                i = 999
              }
              i += 1
            }

          }
          if( localBestFit <= arr(0).fitness){
            arr(0).position  =  localBest
            arr(0).fitness   =  localBestFit
          }
        }

        val chooseSolutios : Int = arr.length * localSearchEvals / 100
        val res = arr.sortWith(_.fitness < _.fitness).take(chooseSolutios)

        val tmp = scala.util.Random.shuffle( 1 to  chooseSolutios - 1).take(mr - 1 )
        var res1 : Array[BAT1] =Array( res(0) )

       for(i <- 0 until tmp.length) {
          res1 = res1 :+ res(tmp(i))
       }

        val writer = new FileWriter(Path + "data/" + index + ".txt")
        for (  i  <-  0  until  arr.length    ) {
          writer.write("("+arr(i).position.toList + "," + arr(i).velocity.toList + "," + arr(i).fitness + "," +
            arr(i).LoudnessRate + "," + arr(i).PulseRate+")" + "\n")
        }
        writer.close()




        val bests : Array[(BAT1 , Int)] = res1.map(x => (x, index))
        bests.toIterator
      }
    }
    newRDD.persist().collect()
  }



  def broadcaster(bests: List[(BAT1, Int)]): Unit = {

    bestSolutions.update(bests.toArray)
  }

  val t0 = System.nanoTime()
  var timeDiff: Double = 0.0


  def Stop() {
    val t1 = System.nanoTime()
    timeDiff = ((t1 - t0).toDouble / 1000000000).toDouble
    println("Total Time Consumed		:		" + ((t1 - t0).toDouble / 1000000000).toDouble + "	Seconds")
  }

}


