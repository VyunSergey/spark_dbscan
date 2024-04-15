package org.alitouka.spark.dbscan.util

import org.alitouka.spark.dbscan.SuiteBase
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD

class PointIndexerSuite extends SuiteBase {
  val points: RDD[String] = sc.parallelize(Array("a", "b", "c", "d", "e", "f", "g", "h", "i"), 3)
  val numPoints: Long = points.count()

  test("Point indexer should calculate unique indexes") {
    val indexer1 = new PointIndexer(2, 1)

    indexer1.multiplier should be (10)
    indexer1.getNextIndex should be (11)
    indexer1.getNextIndex should be (21)
    indexer1.getNextIndex should be (31)

    val indexer2 = new PointIndexer(120, 2)

    indexer2.multiplier should be (1000)
    indexer2.getNextIndex should be (1002)
    indexer2.getNextIndex should be (2002)
    indexer2.getNextIndex should be (3002)
  }

  test("Each point in a dataset should be assigned a unique numeric index") {
    val partitions: Array[Partition] = points.partitions
    val numPartitions: Int = partitions.length

    val indexedPoints = points.mapPartitionsWithIndex { case (partitionIndex, points) =>
      val pointIndexer = new PointIndexer(numPartitions, partitionIndex)

      println (s"mapPartitions called with partitionIndex = $partitionIndex")
      points.map { pt =>
        val pointIndex = pointIndexer.getNextIndex
        (pointIndex, pt)
      }
    }

    val filteredPoints: Array[((Long, String), (Long, String))] = indexedPoints.cartesian(indexedPoints)
      .filter(x => x._1._1 < x._2._1)
      .collect()

    val numFilteredPoints: Int = filteredPoints.length

    numFilteredPoints should be (numPoints * (numPoints - 1) / 2)
  }
}
