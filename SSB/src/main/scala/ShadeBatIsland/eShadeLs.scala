package ShadeBatIsland

import scala.util.Random

class eShadeLs ( fun : Array[ Double] => Double , ub : Int , lb : Int ) extends Serializable  {
  def shadeLs(pop : Array[Array [Double]] ,best: Array[Double], bestInd: Int, bestFit: Double, maxevals: Int , islandSize : Int , evolutions : Int   ): (Array[Double], Double) = {

    var totalevals = 0
    var score: Double = bestFit
    var bestArr = best.clone()
    var FitBest = bestFit
    val wmax = 0.2
    val wmin = 0
    val rnd = new Random()
    var r2 : Double = math.random

    while (totalevals < maxevals) {

      var j = 0
      while (j < best.length) {
        val mu = bestArr.clone()
        var k0 = rnd.nextInt(islandSize - 1)
        var n = rnd.nextInt(best.length - 1)
        while (k0 == bestInd) {
          k0 = rnd.nextInt(islandSize - 1)
        }
        while (n == j) {
          n = rnd.nextInt(best.length - 1)
        }
        r2 = wmin + (((evolutions +  totalevals ).toDouble / 3000000) * (wmax - wmin))
        if (rnd.nextDouble <= r2 )
          mu(j) = bestArr(n) + (2 * rnd.nextDouble - 1) * (bestArr(n) -  pop(k0)(n) )//pop(k0)(n))
        else
          mu(j) = bestArr(j) + (2 * rnd.nextDouble - 1) * (bestArr(n) - pop(k0)(n) )

        // making sure a gen isn't out of boundary
        if (mu(j) > ub)
          mu(j) = (ub + bestArr(j)) / 2
        else if
        (mu(j) < lb) mu(j) = (lb + bestArr(j)) / 2

        score = fun(mu)
        totalevals += 1
        if (score <= FitBest) {
          bestArr = mu
          FitBest = score
        }
        j += 1
      }
    }
    (bestArr , FitBest)
  }
}
