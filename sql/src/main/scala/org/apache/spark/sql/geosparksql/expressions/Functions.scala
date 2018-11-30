/*
 * FILE: Functions.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.apache.spark.sql.geosparksql.expressions

import com.vividsolutions.jts.geom.PrecisionModel
import com.vividsolutions.jts.geom.MultiLineString
import com.vividsolutions.jts.geom.LineString
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.operation.valid.IsValidOp
import com.vividsolutions.jts.precision.GeometryPrecisionReducer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, IntegerType}
import org.apache.spark.unsafe.types.UTF8String
import org.datasyslab.geosparksql.utils.GeometrySerializer
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.operation.MathTransform
import com.vividsolutions.jts.geom._

/**
  * Return the distance between two geometries.
  *
  * @param inputExpressions This function takes two geometries and calculates the distance between two objects.
  */
case class ST_Distance(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {

  // This is a binary expression
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def toString: String = s" **${ST_Distance.getClass.getName}**  "

  override def children: Seq[Expression] = inputExpressions

  override def eval(inputRow: InternalRow): Any = {
    assert(inputExpressions.length == 2)

    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = GeometrySerializer.deserialize(leftArray)

    val rightGeometry = GeometrySerializer.deserialize(rightArray)

    return leftGeometry.distance(rightGeometry)
  }

  override def dataType = DoubleType
}

/**
  * Return the convex hull of a Geometry.
  *
  * @param inputExpressions
  */
case class ST_ConvexHull(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(geometry.convexHull()))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the bounding rectangle for a Geometry
  *
  * @param inputExpressions
  */
case class ST_Envelope(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(geometry.getEnvelope()))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the length measurement of a Geometry
  *
  * @param inputExpressions
  */
case class ST_Length(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    return geometry.getLength
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return the area measurement of a Geometry.
  *
  * @param inputExpressions
  */
case class ST_Area(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    return geometry.getArea
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return mathematical centroid of a geometry.
  *
  * @param inputExpressions
  */
case class ST_Centroid(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(geometry.getCentroid()))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Given a geometry, sourceEPSGcode, and targetEPSGcode, convert the geometry's Spatial Reference System / Coordinate Reference System.
  *
  * @param inputExpressions
  */
case class ST_Transform(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length >= 3 && inputExpressions.length <= 5)
    System.setProperty("org.geotools.referencing.forceXY", "true")
    if (inputExpressions.length >= 4) {
      System.setProperty("org.geotools.referencing.forceXY", inputExpressions(3).eval(input).asInstanceOf[Boolean].toString)
    }
    val originalGeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val sourceCRScode = CRS.decode(inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString)
    val targetCRScode = CRS.decode(inputExpressions(2).eval(input).asInstanceOf[UTF8String].toString)
    var transform: MathTransform = null
    if (inputExpressions.length == 5) {
      transform = CRS.findMathTransform(sourceCRScode, targetCRScode, inputExpressions(4).eval(input).asInstanceOf[Boolean])
    }
    else {
      transform = CRS.findMathTransform(sourceCRScode, targetCRScode, false)
    }
    new GenericArrayData(GeometrySerializer.serialize(JTS.transform(originalGeometry, transform)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return the intersection shape of two geometries. The return type is a geometry
  *
  * @param inputExpressions
  */
case class ST_Intersection(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false
  lazy val GeometryFactory = new GeometryFactory()
  lazy val emptyPolygon = GeometryFactory.createPolygon(null, null)

  override def eval(inputRow: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val leftgeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData])
    val rightgeometry = GeometrySerializer.deserialize(inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData])

    val isIntersects = leftgeometry.intersects(rightgeometry)
    val isLeftContainsRight = leftgeometry.contains(rightgeometry)
    val isRightContainsLeft = rightgeometry.contains(leftgeometry)

    if(!isIntersects) {
      return new GenericArrayData(GeometrySerializer.serialize(emptyPolygon))
    }

    if (isIntersects && isLeftContainsRight) {
      return new GenericArrayData(GeometrySerializer.serialize(rightgeometry))
    }

    if (isIntersects && isRightContainsLeft) {
      return new GenericArrayData(GeometrySerializer.serialize(leftgeometry))
    }

    return new GenericArrayData(GeometrySerializer.serialize(leftgeometry.intersection(rightgeometry)))
  }
  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Test if Geometry is valid.
  *
  * @param inputExpressions
  */
case class ST_IsValid(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    if (inputExpressions(0).eval(input).asInstanceOf[ArrayData] == null) {
      return null
    }
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val isvalidop = new IsValidOp(geometry)
    isvalidop.isValid
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Reduce the precision of the given geometry to the given number of decimal places
  * @param inputExpressions The first arg is a geom and the second arg is an integer scale, specifying the number of decimal places of the new coordinate. The last decimal place will
  *                         be rounded to the nearest number.
  */
case class ST_PrecisionReduce(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val precisionScale = inputExpressions(1).eval(input).asInstanceOf[Int]
    val precisionReduce = new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)))
    new GenericArrayData(GeometrySerializer.serialize(precisionReduce.reduce(geometry)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Returns the count of this Geometrys vertices
  *
  * @param inputExpressions
  */
case class ST_NumPoints(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    return geometry.getNumPoints
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Returns the number of interior rings of a polygon geometry.
  *
  * @param inputExpressions
  */
case class ST_NumInteriorRings(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    if(geometry.isInstanceOf[Polygon]){
      return geometry.asInstanceOf[Polygon].getNumInteriorRing
    }
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Returns the number of Geometries in a GeometryCollection
  *
  * @param inputExpressions
  */
case class ST_NumGeometries(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    return geometry.getNumGeometries
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Returns an element Geometry from a GeometryCollection
  * @param inputExpressions The first arg is a geom and the second arg is an index, specifying the index of the
  * geometry element.
  */
case class ST_GeometryN(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val index = inputExpressions(1).eval(input).asInstanceOf[Int]
    new GenericArrayData(GeometrySerializer.serialize(geometry.getGeometryN(index)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Test if Geometry is ring.
  *
  * @param inputExpressions
  */
case class ST_IsRing(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    if (inputExpressions(0).eval(input).asInstanceOf[ArrayData] == null) {
      return null
    }
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    if(geometry.isInstanceOf[MultiLineString]){
      return geometry.isSimple && geometry.asInstanceOf[MultiLineString].isClosed
    }
    if(geometry.isInstanceOf[LineString]){
      return geometry.isSimple && geometry.asInstanceOf[LineString].isClosed
    }
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Points contained in one Geometry that are not contained in the other Geometry. The return type is a geometry
  *
  * @param inputExpressions
  */
case class ST_Difference(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val leftgeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val rightgeometry = GeometrySerializer.deserialize(inputExpressions(1).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(leftgeometry.difference(rightgeometry)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * points in one Geometry which are not contained in the other Geometry, with the points
  * in the other Geometry not contained in this Geometry. The return type is a geometry
  *
  * @param inputExpressions
  */
case class ST_SymmetricDifference(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val leftgeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val rightgeometry = GeometrySerializer.deserialize(inputExpressions(1).eval(input).asInstanceOf[ArrayData])
    new GenericArrayData(GeometrySerializer.serialize(leftgeometry.symDifference(rightgeometry)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}