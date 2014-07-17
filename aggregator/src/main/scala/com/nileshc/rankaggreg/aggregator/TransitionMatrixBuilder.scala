package com.nileshc.rankaggreg.aggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer

/**
 * Created by nilesh on 9/5/14.
 */
class TransitionMatrixBuilder(sc: SparkContext) {

  import TransitionMatrixBuilder._

  def run(rankListLines: RDD[String], numLists: Long, numPartitions: Int) = {
    //  RDD[(Long, Seq[(Long, Double)])] = {
    val rankLists = rankListLines.map(s => {
      s.drop(4).split(" +").map(_.trim.toLong).toArray
    })
//    println(rankLists.collect().map(_.mkString(",")).mkString("\n"))

    val rankListMatrix = rankLists.mapPartitionsWithIndex {
      case (splitId: Int, iterator: Iterator[Array[Long]]) =>
        var index = 0
        val zippedList = new ListBuffer[(Long, Array[Long])]()
        while(iterator.hasNext) {
          zippedList += (((splitId * 10000000 + index).toLong, iterator.next()))
          index += 1
        }
        zippedList.iterator
    }

    println(rankListMatrix.count())
    val b = rankLists.collect()
    val a = rankListMatrix.collect()
    val temp = a.map(_._2)
    b.foreach(x => println("B " + x.mkString(",")))
    temp.foreach(x => println("A " + x.mkString(",")))
    println(b.filter(temp sameElements _).mkString("\n"))
    println(a.map(x => x._1 + " => " + x._2.mkString(",")).mkString("\n"))

    val itemsRDD = rankListMatrix.flatMap(s => s._2).distinct()
    val items = itemsRDD.collect().toArray
    val numItems = items.length
    val numItemsBC = sc.broadcast(numItems)
    println(numItemsBC.value)

    val threshold = if (numItems % 2 == 0) numItems / 2 else numItems / 2 + 1

    println(rankListMatrix.collect().map(x => (x._1, x._2.mkString("\n"))).mkString("\n"))
    val map = computeRankListEntropies(rankListMatrix, numLists)
    val rankListEntropiesBC = sc.broadcast(map)
    println(map.mkString("\n"))

    val rankedPairs: RDD[((Long, Long), Double)] = rankListMatrix.flatMap {
      case (key: Long, rankList: Array[Long]) =>
        val entropy = rankListEntropiesBC.value(key)
        val indexedRankList = new Array[(Long, Long)](rankList.length)
        var i = 0
        while (i < rankList.length) {
          indexedRankList(i) = (i, rankList(i))
          i += 1
        }
        for ((rank1, elem1) <- indexedRankList; (rank2, elem2) <- indexedRankList)
        yield {
          ((elem1, elem2), if (rank2 < rank1) entropy / math.log(numItemsBC.value) else 0.0)
        }
    }.groupByKey().flatMap {
      case (rowCol: (Long, Long), scores: Seq[Double]) =>
        // Divide entropy sum by numRank
        val transitionMatrixElement = scores.sum / scores.length
        if (transitionMatrixElement == 0.0) {
          if (rowCol._1 == rowCol._2) Seq((rowCol, 0.0)) else Nil
        } else {
          Seq((rowCol, transitionMatrixElement / numItemsBC.value))
        }
    }

    //    rankedPairs.collect.foreach(x => println(x._1._1, x._1._2, x._2.mkString(",")))
    val diagonalElements = rankedPairs.groupBy(x => x._1._1).mapValues(_.map(_._2).sum).map {
      case (row: Long, sumExceptDiagonalElements: Double) =>
        val diagonalElement = 1 - sumExceptDiagonalElements
        ((row, row), diagonalElement)
    }

    val transitionMatrix = (rankedPairs ++ diagonalElements).reduceByKey(_ + _)

    transitionMatrix
  }
}

object TransitionMatrixBuilder {
  def computeRankListEntropies(rankListMatrix: RDD[(Long, Array[Long])], numLists: Long) = {
    val rankListPermutations = rankListMatrix.cartesian(rankListMatrix)

    val kendallDistancePairs = rankListPermutations.map {
      case (list1: (Long, Array[Long]), list2: (Long, Array[Long])) =>
        (Set(list1._1, list2._1), if (list1._1 == list2._1) 0 else kendallTauDistance(list1._2, list2._2))
    }.collectAsMap()

    // The PMF for the i'th iteration and rank list candidate_rank in the entropy formula is as follows:
    // kendalltau_dist(ranks[i], candidate_rank) / sum(for (rank <- ranks) yield kendalltau_dist(rank, candidate_rank))

    val kendallDistWithAll = (
      for (i <- 0l until numLists)
      yield {
        (i, (0l until numLists).foldLeft(0l) {
          (sum, element) => sum + (if (i != element) kendallDistancePairs(Set(i, element)) else 0l)
        })
      }
      ).toMap

    // Probability Mass Function
    def p(loopIndex: Long, candidateListIndex: Long) =
      kendallDistancePairs(Set(loopIndex, candidateListIndex)).toDouble / kendallDistWithAll(candidateListIndex)

    // Calculate entropies for each rank list
    val rankListEntropies = (for (candidate <- 0l until numLists) yield {
      val entropy = (0l until numLists).foldLeft(0.0) {
        (sum, i) =>
          val pValue = p(loopIndex = i, candidateListIndex = candidate)
          sum + (if (pValue == 0.0) 0.0 else -pValue * math.log(pValue))
      }
      (candidate, entropy)
    }).toMap

    rankListEntropies
  }

  def kendallTauDistance(xArray: Array[Long], yArray: Array[Long]): Int = {
    var n = xArray.length
    var swaps = 0
    var extraPenalty = 0
    var xArrayStripped = xArray
    var yArrayStripped = yArray

//    println("xArrayStripped:" + xArrayStripped.mkString(","))
//    println("yArrayStripped:" + yArrayStripped.mkString(","))

    // Refer to http://researcher.watson.ibm.com/researcher/files/us-fagin/topk.pdf
    // p is the penalty score between 0 and 1 inclusive
    val p = 1.0

    xArrayStripped = xArray.intersect(yArray)
    yArrayStripped = yArray.intersect(xArrayStripped)
    n = xArrayStripped.length

//    println("xArray:" + xArrayStripped.mkString(","))
//    println("yArray:" + yArrayStripped.mkString(","))

    // Find the set differences
    val xExtrasLength = xArray.filterNot(yArray contains _).length
    val yExtrasLength = yArray.filterNot(xArray contains _).length

    val case2 = xExtrasLength * yArrayStripped.length + yExtrasLength * xArrayStripped.length
    extraPenalty += case2
//    println("CASE 2:" + case2)

    val case3 = xExtrasLength * yExtrasLength
    extraPenalty += case3
//    println("CASE 3:" + case3)

    // Special case
    val case4 = (p * ((xExtrasLength * (xExtrasLength - 1)) / 2 + (yExtrasLength * (yExtrasLength - 1)) / 2)).toInt
    extraPenalty += case4
//    println("CASE 4:" + case4)

    var pairs = (xArrayStripped zip yArrayStripped).sorted
    var pairsDestination = new Array[(Long, Long)](n)

    var segmentSize = 1
    while (segmentSize < n) {
      var offset = 0
      while (offset < n) {
        var i = offset
        val iEnd = math.min(i + segmentSize, n)
        var j = iEnd
        val jEnd = math.min(j + segmentSize, n)
        var copyLocation = offset
        while (i < iEnd || j < jEnd) {
          if (i < iEnd) {
            if (j < jEnd) {
              if (pairs(i)._2.compare(pairs(j)._2) <= 0) {
                pairsDestination(copyLocation) = pairs(i)
                i += 1
              }
              else {
                pairsDestination(copyLocation) = pairs(j)
                j += 1
                swaps += iEnd - i
              }
            }
            else {
              pairsDestination(copyLocation) = pairs(i)
              i += 1
            }
          }
          else {
            pairsDestination(copyLocation) = pairs(j)
            j += 1
          }
          copyLocation += 1
        }

        offset += 2 * segmentSize
      }

      val pairsTemp = pairs
      pairs = pairsDestination
      pairsDestination = pairsTemp
      segmentSize <<= 1
    }
//    println("CASE 1:" + swaps)
    swaps + extraPenalty
  }
}
