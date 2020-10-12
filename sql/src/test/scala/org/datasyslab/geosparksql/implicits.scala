package org.datasyslab.geosparksql

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.apache.spark.sql.DataFrame

object implicits {
  implicit class DataFrameEnhancer(df: DataFrame){
    def toSeq[T]: Seq[T] =
      df.collect().toSeq.map(element => element(0).asInstanceOf[T]).toList
    def toSeqOption[T]: Option[T] = {
      df.collect().headOption
        .map(element => if (element(0) != null) element(0).asInstanceOf[T] else None.asInstanceOf[T])
    }
  }

  implicit class GeometryFromString(wkt: String){
    def toGeom: Geometry = {
      val wkbReader = new WKTReader()
      wkbReader.read(wkt)
    }

  }

}
