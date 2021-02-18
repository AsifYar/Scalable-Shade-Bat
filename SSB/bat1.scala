package ShadeBatIsland80

import java.io.FileWriter
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.util.Random

case class bat1(  Pop : Int ,  dimension : Int ,  totalPartitions : Int ,  Min : Int ,  Max : Int ,  fun :    Array[Double]    =>  Double  ,
            scc : SparkContext ,  BW : Double , a : Double  , g : Double , IPR : Double ,
            itrs : Int , Frequency : Int , Rate : Int  , rddBat : RDD[BAT1]  , thresh : Int , Path : String , minF : Double , maxF : Double,
                  Lrate : Double , Prate : Double , fId : Int , NP : Int , localSearchEvals : Int ) extends  Serializable {




  lazy val tmpV = Array.fill(dimension)( math.random)
  lazy val tmpP =  Array.fill(dimension)(math.random)
  val arr1 = Array.fill(10)(new BAT1(tmpP,tmpV   ,fun  , fun (tmpP) , Lrate ,Prate) )  
  var GBest  =  arr1.minBy(_.fitness)
  val rnd = new Random()
  val eShadels = new eShadeLs(fun , Max , Min)
  val x = Calendar.getInstance().getTime
  println()


  val theExecutor  =  new Executor1  (  scc ,  BW  , a , g , IPR , Pop , dimension ,
    totalPartitions , Min , Max , fun  , Frequency ,Rate  ,itrs , minF , maxF ,Lrate ,Prate ,thresh , NP ,
    localSearchEvals  , Path)

  var generations : Int  =  0
  var Bests  :  List[(  Int,  Double  )] =  List()

  @tailrec
  final def loop( iteration: Int , totalIterations : Int): Unit = {

    if (iteration >= totalIterations ) theExecutor.Stop()

    else {

      val randomIsland = rnd.nextInt(NP)
      val selected = theExecutor.execute(rddBat, iteration  ,   randomIsland)


      val bests = selected.minBy(_._1.fitness)._1

	    GBest = bests
      Bests = (generations + Frequency, GBest.fitness) :: Bests
      generations += Frequency
      theExecutor.broadcaster(selected.toList)


      println("Best Fitness after " + generations + " generations is = " + GBest.fitness)
      loop(generations, totalIterations)
      }
  }

  val resultRDD  =   loop( generations  , itrs)


  val fOutput = "\n*****************************\n" +  
  println(fOutput)

  val writer = new FileWriter(Path + "ISB.txt", true)
  writer.write(fOutput)

  for (  i  <-  0  until  Bests.length    )
    writer.write(Bests(i)._1+"	,	"+ Bests(i)._2 + "\n")

  writer.write("			-------------------------------------------			")
  writer.close()
}


