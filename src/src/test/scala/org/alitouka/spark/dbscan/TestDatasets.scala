package org.alitouka.spark.dbscan

import org.alitouka.spark.dbscan.spatial.Point
import org.apache.spark.rdd.RDD

trait TestDatasets extends SuiteBase {

  val dataset00: RDD[Point] = createRDDOfPoints(sc,
    (1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (3.0, 2.0),
    (5.0, 4.0), (6.0, 4.0), (6.0, 5.0),
    (6.0, 1.0), (1.0, 5.0), (2.0, 5.0)
  )

  val dataset01: RDD[Point] = createRDDOfPoints(sc,
    ( 1.0,  1.0), ( 2.0,  1.0), ( 2.0,  2.0), ( 3.0,  2.0),
    (15.0, 14.0), (16.0, 14.0), (16.0, 15.0),
    (16.0, 11.0), ( 1.0,  5.0), ( 2.0,  5.0)
  )

  val dataset10: RDD[Point] = createRDDOfPoints(sc,
    (0.0, 0.0), (1.0, 0.0),
    (0.5, 1.0), (0.5, 0.5)
  )

  val dataset11: RDD[Point] = createRDDOfPoints(sc,
    (0.0, 0.0), (1.0, 0.0), (0.5, 1.0),
    (0.5, 0.5), (0.5, 5.0)
  )

  val dataset20: RDD[Point] = createRDDOfPoints(sc,
    (0.0, 0.0), (0.0, 1.0), (0.5, 0.5), (1.0, 0.5),
    (1.5, 0.5), (2.0, 0.0), (2.0, 1.0)
  )

  var dataset30: RDD[Point] = createRDDOfPoints(sc,
    (0.0, 1.0), (1.0, 1.0), (3.0, 1.0), (4.0, 1.0), (0.5, 0.5),
    (0.5, 1.5), (3.5, 0.5), (3.5, 1.5), (0.0, 3.0), (1.0, 3.0),
    (3.0, 3.0), (4.0, 3.0), (0.5, 2.5), (3.5, 2.5), (0.0, 4.0),
    (1.0, 4.0), (3.0, 4.0), (4.0, 4.0), (0.5, 3.5), (3.5, 3.5)
  )

  dataset30 ++= sc.parallelize((0 to 4).map(x => create2DPoint(x, 0.0).withPointId(100+x)))
  dataset30 ++= sc.parallelize((0 to 4).map(x => create2DPoint(x, 2.0).withPointId(200+x)))
}
