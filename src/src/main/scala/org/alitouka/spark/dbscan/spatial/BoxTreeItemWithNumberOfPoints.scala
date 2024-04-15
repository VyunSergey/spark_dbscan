package org.alitouka.spark.dbscan.spatial

private [dbscan] class BoxTreeItemWithNumberOfPoints(b: Box) extends BoxTreeItemBase[BoxTreeItemWithNumberOfPoints](b) {
  var numberOfPoints: Long = 0

  override def clone(): BoxTreeItemWithNumberOfPoints  = {
    val result = new BoxTreeItemWithNumberOfPoints (this.box)
    result.children = this.children.map(_.clone()).toList
    result
  }
}
