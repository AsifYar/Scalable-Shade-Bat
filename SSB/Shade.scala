package ShadeBatIsland80
import scala.util.Random

class Shade (  dimension : Int  , lower : Int , upper : Int , fun : Array[Double] => Double , mr : Int , N :Int) extends Serializable  {
  def improve(evalutions : Int , pos:Array[Array[Double]]) :  (Array[Double] , Array[Array[Double]] )  = {
    val population = pos 
    var memory = population.toList 
    val size = population.length
    val memorySize = size
    val H = 100
    val MemF = Array.fill(H)(0.55)
    val MemCR = Array.fill(H)(0.95)
    var k = 0
    val pmin = 2.0 / size
    val population_fitness = population map fun
    val maxEval = evalutions
    var currentEval = 0
    val r = new Random

    while (currentEval < maxEval) {
      var SCR = Array[Double]()
      var SF  = Array[Double]()
      val F = Array.fill(size)(0.55)
      val CR = Array.fill(size)(0.95 )
      val u = Array.fill(size, dimension)(0.0)
      var best_fitness = population_fitness.min
      var numEvalFound = currentEval
      for ((i, xi) <- population.view.zipWithIndex) {
        val index_H =  lower + r.nextInt( (upper - lower) + 1 )
        val index_H = r.nextInt(H)
        val meanF = MemF(index_H)

        val meanCR = MemCR(index_H)
        val Fi = r.nextGaussian() * 0.1 + meanF
        val CRi =  r.nextGaussian() * 0.1 + meanCR
        val p = math.random * (0.2 - pmin) + pmin
        val ign = Array(xi)
        val r1: Int = randomIndices(size, 1, ign)(0)

        while  (  r1  ==  r2  ||  r2  ==  xi  ||  r1   ==  xi )  {
          r1  =  r.nextInt (size)
          r2  =  r.nextInt( memory.length )
        }
        
        val ign1 = Array(xi, r1)
        val r2: Int = randomIndices(memory.length, 1, ign1)(0)
        val xr1 = population(r1)
        val xr2 = memory(r2)

        var maxbest: Int = (p * size).toInt
        if (maxbest <= 0)  maxbest = 1

        val bests = population_fitness.zipWithIndex.sorted.map(_._2) take maxbest
        val pbest = bests(r.nextInt(bests.length))
        val xbest = population(pbest)
        
        var v = ES(i, ESt(xbest, i, Fi), ESt(xr1, xr2, Fi))
        v = shade_clip(v, i) 

      
        val idxchange: Array[Boolean] = Array.fill(dimension)(math.random).map(x => x < CRi)
        u(xi) = i.clone()
        val indexes = idxchange.zipWithIndex filter (x => x._1) map (_._2)
        for (z <- indexes.indices) {
          u(xi)(indexes(z)) = v(indexes(z))

        }

        F(xi) = Fi
        CR(xi) = CRi
      } 
      var weights = Array[Double]()

      for ((fitness, i) <- population_fitness.view.zipWithIndex) {
        val fitness_u = fun(u(i))
        assert(fitness_u != Double.NaN, "illegal fitness")

        if (fitness_u <= fitness) {
          if (fitness_u < fitness) {
            memory = memory :+ population(i)
            SF = SF :+ F(i) 
            SCR = SCR :+ CR(i)
            weights = weights :+ (fitness - fitness_u)
          }
          if (fitness_u < best_fitness) {
            best_fitness = fitness_u
            numEvalFound = currentEval
          }
          population(i) = u(i)
          population_fitness(i) = fitness_u
        }

      }
      currentEval += N
      if (memory.length > memorySize) {
        memory = r.shuffle(memory).take(memorySize)
      }
      if (SCR.length > 0 & SF.length > 0) {
        val (fNew, crNew) = update_FCR(SF, SCR, weights)
        MemF(k) = fNew
        MemCR(k) = crNew
        k = (k + 1) % H
      }
    }

    (population_fitness , population)

  }


  def update_FCR(SF : Array[Double], SCR : Array[Double], improvements : Array[Double]):(Double , Double) = {
    val total = improvements.reduce(_+_)
    assert ( total > 0)
    val weights = improvements .map( x => x / total)
    var Fnew = ElementWiseProduct3Arrays(weights,SF,SF).reduce(_+_)   / ElementWiseProduct2Arrays(weights , SF).reduce(_+_)
    Fnew  =  keepWithinRange(Fnew , 0 , 1)
    var CRnew =  ElementWiseProduct2Arrays(weights  , SCR) . reduce(_+_)
    CRnew = keepWithinRange(CRnew, 0, 1)
    (Fnew , CRnew)
  }

  def ElementWiseProduct2Arrays(arr: Array[Double], arr1: Array[Double]): Array[Double] = {
    val newArr = new Array[Double](arr.length)
    var i = 0
    while (i < arr.length) {
      newArr(i) = arr(i) * arr1(i)
      i += 1
    }
    newArr
  }

  def ElementWiseProduct3Arrays(arr: Array[Double], arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    val newArr = new Array[Double](arr.length)
    var i = 0
    while (i < arr.length) {
      newArr(i) = arr(i) * arr1(i) * arr2(i)
      i += 1
    }

    newArr
  }

  def shade_clip( solution : Array[Double], original :Array[Double]) : Array[Double] ={
    val clip_sol = solution. map ( s => keepWithinRange(s , lower , upper))
    if ( solution.sameElements(clip_sol) ) solution
    else {

      val done = solution.indices.map{x =>
        if (solution(x) < lower)      (lower + original(x))/2.0
        else if (solution(x) > upper) (upper + original(x))/2.0
        else solution(x)
      }
      done
    }.toArray
  }


  def keepWithinRange(x: Double, min: Int, max: Int) : Double = math.max(min, math.min(max, x))

  def ES(arr: Array[Double], arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    val newArr =new Array[Double](arr.length)
    var i = 0
    while (i < arr.length) {
      newArr(i) = arr(i) + arr1(i) + arr2(i)
      i += 1
    }
    newArr
  }


  def ESt(arr: Array[Double], arr1: Array[Double], frq: Double): Array[Double] = {
    val newArr = new Array[Double](arr.length)
    var i = 0
    while (i < arr.length) {
      newArr(i) = (arr(i) - arr1(i)) * frq
      i += 1
    }
    newArr
  }


  def randomIndices(arraySize: Int, nIndices: Int , ignore : Array[Int]): List[Int] = {
    def loop(done: Int, res: List[Int]): List[Int] =
      if (done < nIndices) {
        loop(done + 1, (Math.random * arraySize).toInt +: res)
      } else {

        val d = res . distinct.filterNot(ignore.toSet)
        val dSize = d.size

        if (dSize < nIndices) {
          loop(dSize, d)
        } else {
          res
        }
      }

    if (nIndices > arraySize) {
      randomIndices(arraySize, arraySize , ignore)
    } else {
      loop(0, Nil)
    }

  }
}
