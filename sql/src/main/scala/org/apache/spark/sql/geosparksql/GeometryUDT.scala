package org.apache.spark.sql.geosparksql

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.datasyslab.geospark.geometryObjects.GeometrySerde


private[sql] class GeometryUDT extends UserDefinedType[GeometryWrapper]{
  override def sqlType: DataType = ArrayType(ByteType, false)

  override def userClass: Class[GeometryWrapper] = classOf[GeometryWrapper]

  override def serialize(obj: GeometryWrapper): GenericArrayData =
  {
    val out = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val geometrySerde = new GeometrySerde()
    val output = new Output(out)
    geometrySerde.write(kryo,output,obj.getGeometry())
    output.close()
    new GenericArrayData(out.toByteArray)
  }

  override def deserialize(datum: Any): GeometryWrapper =
  {
    datum match
    {
      case values: ArrayData => {
        val in = new ByteArrayInputStream(values.toByteArray())
        val kryo = new Kryo()
        val geometrySerde = new GeometrySerde()
        val input = new Input(in)
        val geometry = geometrySerde.read(kryo, input,classOf[Geometry])
        input.close()
        return new GeometryWrapper(geometry.asInstanceOf[Geometry])
      }
    }
  }

}
