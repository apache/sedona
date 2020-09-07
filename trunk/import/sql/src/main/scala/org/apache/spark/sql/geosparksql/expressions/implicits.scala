package org.apache.spark.sql.geosparksql.expressions

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory, Point}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.datasyslab.geosparksql.utils.GeometrySerializer

object implicits {
  implicit class InputExpressionEnhancer(inputExpression: Expression){
    def toGeometry(input: InternalRow): Geometry = {
      inputExpression.eval(input).asInstanceOf[ArrayData] match {
        case arrData: ArrayData => GeometrySerializer.deserialize(arrData)
        case _ => null
      }
    }
    def toInt(input: InternalRow): Int = {
      inputExpression.eval(input).asInstanceOf[Int]
    }
  }

  implicit class SequenceEnhancer[T](seq: Seq[T]){
    def validateLength(length: Int, message: Option[String] = None): Unit = {
      message match {
        case None => assert(length == seq.length, s"Expression should be $length long")
        case Some(x) => assert(length == seq.length, message)
      }
    }


    def betweenLength(a: Int, b: Int): Unit = {
      val length = seq.length
      assert(length >= a && length <= b)
    }
  }

  implicit class ArrayDataEnhancer(arrayData: ArrayData){
    def toGeometry: Geometry = {
      arrayData match {
        case arrData: ArrayData => GeometrySerializer.deserialize(arrData)
        case _ => null
      }
    }
  }

  implicit class GeometryEnhancer(geom: Geometry){
    private val geometryFactory = new GeometryFactory()
    def toGenericArrayData: GenericArrayData =
      new GenericArrayData(GeometrySerializer.serialize(geom))

    def getPoints: Array[Point] =
      geom.getCoordinates.map(coordinate => geometryFactory.createPoint(coordinate))
  }
}
