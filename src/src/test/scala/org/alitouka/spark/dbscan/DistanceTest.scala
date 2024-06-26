package org.alitouka.spark.dbscan

import org.apache.commons.math3.ml.distance.EuclideanDistance
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.WrappedArray.ofDouble

class DistanceTest extends AnyFunSuite with Matchers {
  test("Euclidean distance should be equal to square root of ...") {

    val pt1 = new ofDouble(Array(0.0, 0.0))
    val pt2 = new ofDouble(Array(1.0, 1.0))
    val distance = new EuclideanDistance().compute(pt1.toArray, pt2.toArray)

    distance should be (math.sqrt(2))
  }
}
