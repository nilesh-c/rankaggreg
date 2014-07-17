package com.nileshc.rankaggreg.aggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable

/**
 * Created by nilesh on 9/5/14.
 */
class DworkTransitionMatrixBuilder(sc: SparkContext) {
  def run(rankListLines: RDD[String], numPartitions: Int) = {
    //  RDD[(Long, Seq[(Long, Double)])] = {
    val numListsAccm = sc.accumulator(0)
    val rankLists = rankListLines.map(s => {
      numListsAccm += 1
      s.split(",").map(_.trim.toLong).toArray
    })

    println(rankLists.collect().map(_.mkString(",")).mkString("\n"))

    val numLists = numListsAccm.value
    val rankListMatrix = sc.parallelize(0 until numLists, numPartitions) zip rankLists
    println(rankListMatrix.collect().mkString("\n"))

    val itemsRDD = rankListMatrix.flatMap(s => s._2).distinct()
    val items = itemsRDD.collect().toArray
    val numItems = items.length
    val numItemsBC = sc.broadcast(numItems)
    println(numItemsBC.value)

    val thresholdBC = sc.broadcast(if (numItems % 2 == 0) numItems.toDouble / 2 else numItems.toDouble / 2 + 1)

//    println(rankListMatrix.collect().map(x => (x._1, x._2.mkString("\n"))).mkString("\n"))
//    val map = computeRankListEntropies(rankListMatrix, numLists)
//    val rankListEntropiesBC = sc.broadcast(map)
//    println(map.mkString("\n"))

    val rankedPairs: RDD[((Long, Long), Double)] = rankListMatrix.flatMap {
      case (key: Int, rankList: Array[Long]) =>
        val indexedRankList = new Array[(Long, Long)](rankList.length)
        var i = 0
        while (i < rankList.length) {
          indexedRankList(i) = (i, rankList(i))
          i += 1
        }
        for ((rank1, elem1) <- indexedRankList; (rank2, elem2) <- indexedRankList)
        yield {
          ((elem1, elem2), if (rank2 < rank1) 1.0 else 0.0)
        }
    }.groupByKey().flatMap{
      case (rowCol: (Long, Long), greaterRankedCount: Seq[Double]) =>
        // Divide entropy sum by numRank
        val transitionMatrixElement = if(greaterRankedCount.sum >= thresholdBC.value) 1.0 else 0.0
        if(transitionMatrixElement == 0.0) {
          if(rowCol._1 == rowCol._2) Seq((rowCol, 0.0)) else Nil
        } else {
          Seq((rowCol, transitionMatrixElement / numItemsBC.value))
        }
    }

    //    rankedPairs.collect.foreach(x => println(x._1._1, x._1._2, x._2.mkString(",")))
    val diagonalElements = rankedPairs.groupBy(x => x._1._1).mapValues(_.map(_._2).sum).map{
      case (row: Long, sumExceptDiagonalElements: Double) =>
        val diagonalElement = 1 - sumExceptDiagonalElements
        ((row, row), diagonalElement)
    }

    val transitionMatrix = (rankedPairs ++ diagonalElements).reduceByKey(_ + _)

    transitionMatrix
  }
}