package org.apache.spark.sql.geosparksql

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.types.SQLUserDefinedType
import org.datasyslab.geospark.enums.{FileDataSplitter, GeometryType}
import org.datasyslab.geospark.formatMapper.FormatMapper

@SQLUserDefinedType(udt = classOf[GeometryUDT])
class GeometryWrapper extends Serializable {

  private var geometry: Geometry = _
  def getGeometry(): Geometry = this.geometry

  def this(geometryString: String, fileDataSplitter: FileDataSplitter, geometryType: GeometryType)
  {
    this()
    var formatMapper = new FormatMapper(fileDataSplitter, false, geometryType)
    this.geometry = formatMapper.readGeometry(geometryString)
  }

  def this(geometryString: String, fileDataSplitter: FileDataSplitter)
  {
    this(geometryString, fileDataSplitter,null)
  }

  def this(geometry: Geometry)
  {
    this()
    this.geometry = geometry
  }
}
