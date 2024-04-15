package org.alitouka.spark.dbscan

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.alitouka.spark.dbscan.spatial.{Point, PointSortKey}
import org.apache.spark.rdd.RDD

class SuiteBase extends AnyFunSuite with Matchers with BeforeAndAfterEach with Logging {
  val sc: SparkContext = TestContextHolder.sc
  sc.setLogLevel("WARN")

  protected def readDataset(path: String): RDD[Point] = {
    val rawData = sc.textFile(path)

    rawData.map { line =>
      new Point(line.split(",").take(2).map(_.toDouble))
    }
  }

  def createRDDOfPoints(sc: SparkContext, points: (Double, Double)*): RDD[Point] = {
    val pointIds = points.indices.map(_ + 1)
    val pointObjects = points.zip(pointIds).map { case ((x, y), id) =>
      create2DPoint(x, y, id)
    }

    sc.parallelize(pointObjects)
  }

  def create2DPoint(x: Double, y: Double, idx: PointId = 0): Point = {
    new Point(new PointCoordinates(Array(x, y)), idx, 1, Math.sqrt(x*x+y*y))
  }

  def create2DPointWithSortKey(x: Double, y: Double, idx: PointId = 0): (PointSortKey, Point) = {
    val pt = create2DPoint(x, y, idx)
    val sortKey = new PointSortKey(pt)

    (sortKey, pt)
  }
}
