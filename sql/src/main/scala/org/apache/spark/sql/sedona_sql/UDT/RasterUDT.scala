package org.apache.spark.sql.sedona_sql.UDT


import org.apache.sedona.common.raster.Serde
import org.apache.spark.sql.types.{BinaryType, DataType, UserDefinedType}
import org.geotools.coverage.grid.GridCoverage2D

class RasterUDT extends UserDefinedType[GridCoverage2D] {
  override def sqlType: DataType = BinaryType

  override def serialize(raster: GridCoverage2D): Array[Byte] = Serde.serialize(raster)

  override def deserialize(datum: Any): GridCoverage2D = {
    datum match {
      case bytes: Array[Byte] => Serde.deserialize(bytes)
    }
  }

  override def userClass: Class[GridCoverage2D] = classOf[GridCoverage2D]
}

case object RasterUDT extends RasterUDT with Serializable