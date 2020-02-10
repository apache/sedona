package org.imbruced.geo_pyspark.serializers

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.util.ArrayData
import org.datasyslab.geosparksql.utils.GeometrySerializer

object GeometryAdapter {
  def deserializeToGeometry(bytes: java.util.ArrayList[Int]): Geometry = {
    val bytesData = bytes.toArray.map(x => x.asInstanceOf[Int].toByte)
    GeometrySerializer.deserialize(ArrayData.toArrayData(bytesData))
  }
}
