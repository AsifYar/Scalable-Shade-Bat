package ShadeBatIsland80
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

import scala.util.Random

 object shadeBat {
  def main(args: Array[String]): Unit = {


    val N =       args(0).toInt 
    val d =       args(1).toInt 
    val Iterations = args(2).toInt 
    val nP =      args(3).toInt 
    val migrationFrequency = args(4).toInt 
    val migrationRate =       args(5).toInt 

 

    val Fmin =    args(6).toDouble 
    val Fmax =    args(7).toDouble 
    val Bandwidth =         args(8).toDouble
    val InitialPulseRate =  args(9).toDouble 
    val PulseRate = InitialPulseRate
    val LoudnessRate =      args(10).toDouble 
    val alpha =             args(11).toDouble 
    val gyma =              args(12).toDouble 
    val th =                args(13).toInt

    var f =                 args(14)
    val localSearchEvals =  args(15).toInt
    val path =              args(16)
    val configs =           args(17) 
    val conf = new SparkConf().setAppName("EvOp").setMaster(configs)
    val sc = new SparkContext(conf)


    if (f.toLowerCase() == "se" || f.toLowerCase() == "shiftedElliptic") f = "ShiftedEllipticFunction"
    if (f.toLowerCase() == "sr" || f.toLowerCase() == "shiftedRastrigin") f = "ShiftedRastriginFunction"
    if (f.toLowerCase() == "sa" || f.toLowerCase() == "shiftedAckley") f = "ShiftedAckleyFunction"
    if (f.toLowerCase() == "sre" || f.toLowerCase() == "shiftedRotatedElliptic") f = "ShiftedRotatedEllipticFunction"
    if (f.toLowerCase() == "srr" || f.toLowerCase() == "shiftedRotatedRastrigin") f = "ShiftedRotatedRastriginFunction"
    if (f.toLowerCase() == "sra" || f.toLowerCase() == "shiftedRotatedAckley") f = "ShiftedRotatedAckleyFunction"
    if (f.toLowerCase() == "srs" || f.toLowerCase() == "shiftedRotatedSchwefel") f = "ShiftedRotatedSchwefelFunction"
    if (f.toLowerCase() == "nssre" || f.toLowerCase() == "nonSeparableShiftedRotatedElliptic") f = "NonSeparableShiftedRotatedEllipticFunction"
    if (f.toLowerCase() == "nssrr" || f.toLowerCase() == "nonSeparableShiftedRotatedRastrigin") f = "NonSeparableShiftedRotatedRastriginFunction"
    if (f.toLowerCase() == "nssra" || f.toLowerCase() == "nonSeparableShiftedRotatedAckley") f = "NonSeparableShiftedRotatedAckleyFunction"
    if (f.toLowerCase() == "nsss" || f.toLowerCase() == "nonSeparableShiftedSchwefeln") f = "NonSeparableShiftedSchwefelFunction"
    if (f.toLowerCase() == "sro" || f.toLowerCase() == "shiftedRosenbrock") f = "ShiftedRosenbrockFunction"
    if (f.toLowerCase() == "coss" || f.toLowerCase() == "conformingOverlappingShiftedSchwefel") f = "ConformingOverlappingShiftedSchwefelFunction"
    if (f.toLowerCase() == "cooss" || f.toLowerCase() == "conflictingOverlappingShiftedSchwefel") f = "ConflictingOverlappingShiftedSchwefelFunction"
    if (f.toLowerCase() == "ss" || f.toLowerCase() == "shiftedSchwefel") f = "ShiftedSchwefelFunction"


    val rnd = new Random
    oFunctions.func = f

    f match {

      case "ShiftedEllipticFunction" => {

        var min = oFunctions.EllipticBound(0)
        var max = oFunctions.EllipticBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
             BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
               , oFunctions.ShiftedElliptic,oFunctions.ShiftedElliptic(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

         bat1(N, d: Int, nP, min, max, oFunctions.ShiftedElliptic, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate,1 , nP , localSearchEvals)
      }
      case "ShiftedRastriginFunction" => {

        var min = oFunctions.RastriginBound(0)
        var max = oFunctions.RastriginBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedRastrigin,oFunctions.ShiftedRastrigin(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedRastrigin, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate,2 , nP , localSearchEvals )
      }

      case "ShiftedAckleyFunction" => {

        var min = oFunctions.AckleyBound(0)
        var max = oFunctions.AckleyBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedAckleyFunction,oFunctions.ShiftedAckleyFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedAckleyFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate,3 ,nP, localSearchEvals )
      }

      case "ShiftedRotatedEllipticFunction" => {

        var min = oFunctions.EllipticBound(0)
        var max = oFunctions.EllipticBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedRotatedEllipticFunction,oFunctions.ShiftedRotatedEllipticFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedRotatedEllipticFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate,4 ,nP, localSearchEvals )
      }

      case "ShiftedRotatedRastriginFunction" => {

        var min = oFunctions.RastriginBound(0)
        var max = oFunctions.RastriginBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedRotatedRastriginFunction,oFunctions.ShiftedRotatedRastriginFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedRotatedRastriginFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate, 5 ,nP, localSearchEvals )
      }

      case "ShiftedRotatedAckleyFunction" => {

        var min = oFunctions.AckleyBound(0)
        var max = oFunctions.AckleyBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedRotatedAckleyFunction,oFunctions.ShiftedRotatedAckleyFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedRotatedAckleyFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 6 ,nP, localSearchEvals )
      }

      case "ShiftedRotatedSchwefelFunction" => {

        var min = oFunctions.SchwefelBound(0)
        var max = oFunctions.SchwefelBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedRotatedSchwefelFunction,oFunctions.ShiftedRotatedSchwefelFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedRotatedSchwefelFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 7 ,nP, localSearchEvals )
      }

      case "NonSeparableShiftedRotatedEllipticFunction" => {

        var min = oFunctions.EllipticBound(0)
        var max = oFunctions.EllipticBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.NonSeparableShiftedRotatedEllipticFunction,oFunctions.NonSeparableShiftedRotatedEllipticFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.NonSeparableShiftedRotatedEllipticFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 8 ,nP , localSearchEvals )
      }

      case "NonSeparableShiftedRotatedRastriginFunction" => {

        var min = oFunctions.RastriginBound(0)
        var max = oFunctions.RastriginBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.NonSeparableShiftedRotatedRastriginFunction,oFunctions.NonSeparableShiftedRotatedRastriginFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.NonSeparableShiftedRotatedRastriginFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate ,9 ,nP , localSearchEvals )
      }

      case "NonSeparableShiftedRotatedAckleyFunction" => {

        var min = oFunctions.AckleyBound(0)
        var max = oFunctions.AckleyBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.NonSeparableShiftedRotatedAckleyFunction,oFunctions.NonSeparableShiftedRotatedAckleyFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.NonSeparableShiftedRotatedAckleyFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 10 ,nP , localSearchEvals )
      }

      case "NonSeparableShiftedSchwefelFunction" => {

        var min = oFunctions.SchwefelBound(0)
        var max = oFunctions.SchwefelBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.NonSeparableShiftedSchwefelFunction,oFunctions.NonSeparableShiftedSchwefelFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.NonSeparableShiftedSchwefelFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate, 11 ,nP , localSearchEvals )
      }

      case "ShiftedRosenbrockFunction" => {

        var min = oFunctions.RosenBound(0)
        var max = oFunctions.RosenBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedRosenbrockFunction,oFunctions.ShiftedRosenbrockFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedRosenbrockFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 12  ,nP , localSearchEvals )
      }

      case "ConformingOverlappingShiftedSchwefelFunction" => {

        var min = oFunctions.SchwefelBound(0)
        var max = oFunctions.SchwefelBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ConformingOverlappingShiftedSchwefelFunction,oFunctions.ConformingOverlappingShiftedSchwefelFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ConformingOverlappingShiftedSchwefelFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 13 ,nP , localSearchEvals )
      }

      case "ConflictingOverlappingShiftedSchwefelFunction" => {

        var min = oFunctions.SchwefelBound(0)
        var max = oFunctions.SchwefelBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ConflictingOverlappingShiftedSchwefelFunction,oFunctions.ConflictingOverlappingShiftedSchwefelFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ConflictingOverlappingShiftedSchwefelFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate ,14 ,nP , localSearchEvals )
      }

      case "ShiftedSchwefelFunction" => {

        var min = oFunctions.SchwefelBound(0)
        var max = oFunctions.SchwefelBound(1)
        val batRDD1 = sc.parallelize(0 until N, nP).mapPartitionsWithIndex { (index, iter) =>
          val data = iter.map { i =>
            val pos = Array.fill(d)(  rnd.nextDouble()*(max-min)+min  )
            BAT1(pos , Array.fill(d)(  rnd.nextDouble  )
              , oFunctions.ShiftedSchwefelFunction,oFunctions.ShiftedSchwefelFunction(pos)  , LoudnessRate, PulseRate)
          }
          data
        }.persist(StorageLevel.MEMORY_AND_DISK)

        new bat1(N, d: Int, nP, min, max, oFunctions.ShiftedSchwefelFunction, sc,
          Bandwidth, alpha, gyma, InitialPulseRate, Iterations, migrationFrequency, migrationRate, batRDD1, th, path, Fmin, Fmax, LoudnessRate, PulseRate , 15 ,nP , localSearchEvals )
      }
    }

  }

}
