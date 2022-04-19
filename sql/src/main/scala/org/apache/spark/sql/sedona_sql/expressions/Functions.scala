/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.core.geometryObjects.Circle
import org.apache.sedona.core.utils.GeomUtils
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, Generator}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.collect.Collect
import org.apache.spark.sql.sedona_sql.expressions.geohash.{GeoHashDecoder, GeometryGeoHashEncoder, InvalidGeoHashException}
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.expressions.subdivide.GeometrySubDivider
import org.apache.spark.sql.types.{ArrayType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.algorithm.MinimumBoundingCircle
import org.locationtech.jts.geom.util.GeometryFixer
import org.locationtech.jts.geom.{PrecisionModel, _}
import org.locationtech.jts.io.{ByteOrderValues, WKBWriter, WKTWriter}
import org.locationtech.jts.linearref.LengthIndexedLine
import org.locationtech.jts.operation.IsSimpleOp
import org.locationtech.jts.operation.buffer.BufferParameters
import org.locationtech.jts.operation.distance3d.Distance3DOp
import org.locationtech.jts.operation.linemerge.LineMerger
import org.locationtech.jts.operation.valid.IsValidOp
import org.locationtech.jts.precision.GeometryPrecisionReducer
import org.locationtech.jts.simplify.TopologyPreservingSimplifier
import org.opengis.referencing.operation.MathTransform
import org.wololo.jts2geojson.GeoJSONWriter

import java.nio.ByteOrder
import scala.util.{Failure, Success, Try}

/**
  * Return the distance between two geometries.
  *
  * @param inputExpressions This function takes two geometries and calculates the distance between two objects.
  */
case class ST_Distance(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_Distance.getClass.getName}**  "

  override def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    leftGeometry.distance(rightGeometry)
  }

  override def dataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_3DDistance(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def toString: String = s" **${ST_3DDistance.getClass.getName}**  "

  override def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    Distance3DOp.distance(leftGeometry, rightGeometry)
  }

  override def dataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return the convex hull of a Geometry.
  *
  * @param inputExpressions
  */
case class ST_ConvexHull(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullSafeEval(geometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(geometry.convexHull()))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return the number of Points in geometry.
  *
  * @param inputExpressions
  */
case class ST_NPoints(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {

  override def nullSafeEval(geometry: Geometry): Any = {
    geometry.getCoordinates.length
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance.
  *
  * @param inputExpressions
  */
case class ST_Buffer(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val buffer: Double = inputExpressions(1).eval(input) match {
      case a: Decimal => a.toDouble
      case a: Double => a
      case a: Int => a
    }
    inputExpressions(0).toGeometry(input) match {
      case geometry: Geometry => geometry.buffer(buffer).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Return the bounding rectangle for a Geometry
  *
  * @param inputExpressions
  */
case class ST_Envelope(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullSafeEval(geometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(geometry.getEnvelope()))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return the length measurement of a Geometry
  *
  * @param inputExpressions
  */
case class ST_Length(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullSafeEval(geometry: Geometry): Any = {
    geometry.getLength
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return the area measurement of a Geometry.
  *
  * @param inputExpressions
  */
case class ST_Area(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullSafeEval(geometry: Geometry): Any = {
    geometry.getArea
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return mathematical centroid of a geometry.
  *
  * @param inputExpressions
  */
case class ST_Centroid(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullSafeEval(geometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(geometry.getCentroid()))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Given a geometry, sourceEPSGcode, and targetEPSGcode, convert the geometry's Spatial Reference System / Coordinate Reference System.
  *
  * @param inputExpressions
  */
case class ST_Transform(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length >= 3 && inputExpressions.length <= 4)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val originalGeometry = inputExpressions(0).toGeometry(input)
    val sourceCRS = inputExpressions(1).asString(input)
    val targetCRS = inputExpressions(2).asString(input)

    (originalGeometry, sourceCRS, targetCRS) match {
      case (originalGeometry: Geometry, sourceCRS: String, targetCRS: String) =>
        val sourceCRScode = CRS.decode(sourceCRS)
        val targetCRScode = CRS.decode(targetCRS)
        var transform: MathTransform = null
        if (inputExpressions.length == 4) {
          transform = CRS.findMathTransform(sourceCRScode, targetCRScode, inputExpressions(3).eval(input).asInstanceOf[Boolean])
        }
        else {
          transform = CRS.findMathTransform(sourceCRScode, targetCRScode, false)
        }
        JTS.transform(originalGeometry, transform).toGenericArrayData
      case (_, _, _) => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Return the intersection shape of two geometries. The return type is a geometry
  *
  * @param inputExpressions
  */
case class ST_Intersection(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  lazy val GeometryFactory = new GeometryFactory()
  lazy val emptyPolygon = GeometryFactory.createPolygon(null, null)

  override def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    val isIntersects = leftGeometry.intersects(rightGeometry)
    lazy val isLeftContainsRight = leftGeometry.contains(rightGeometry)
    lazy val isRightContainsLeft = rightGeometry.contains(leftGeometry)

    if (!isIntersects) {
      return new GenericArrayData(GeometrySerializer.serialize(emptyPolygon))
    }

    if (isIntersects && isLeftContainsRight) {
      return new GenericArrayData(GeometrySerializer.serialize(rightGeometry))
    }

    if (isIntersects && isRightContainsLeft) {
      return new GenericArrayData(GeometrySerializer.serialize(leftGeometry))
    }

    new GenericArrayData(GeometrySerializer.serialize(leftGeometry.intersection(rightGeometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Given an invalid geometry, create a valid representation of the geometry.
  * See: http://lin-ear-th-inking.blogspot.com/2021/05/fixing-invalid-geometry-with-jts.html
  *
  * @param inputExpressions
  */
case class ST_MakeValid(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1 || inputExpressions.length == 2)

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val keepCollapsed = if (inputExpressions.length == 2) {
      inputExpressions(1).eval(input).asInstanceOf[Boolean]
    } else {
      false
    }
    (geometry) match {
      case (geometry: Geometry) => nullSafeEval(geometry, keepCollapsed)
      case _ => null
    }
  }

  private def nullSafeEval(geometry: Geometry, keepCollapsed: Boolean) = {
    val fixer = new GeometryFixer(geometry)
    fixer.setKeepCollapsed(keepCollapsed)
    fixer.getResult.toGenericArrayData
  }

  override def nullable: Boolean = true

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if Geometry is valid.
  *
  * @param inputExpressions
  */
case class ST_IsValid(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)


  override protected def nullSafeEval(geometry: Geometry): Any = {
    new IsValidOp(geometry).isValid
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Test if Geometry is simple.
  *
  * @param inputExpressions
  */
case class ST_IsSimple(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    new IsSimpleOp(geometry).isSimple
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Simplifies a geometry and ensures that the result is a valid geometry having the same dimension and number of components as the input,
  * and with the components having the same topological relationship.
  * The simplification uses a maximum-distance difference algorithm similar to the Douglas-Peucker algorithm.
  *
  * @param inputExpressions first arg is geometry
  *                         second arg is distance tolerance for the simplification(all vertices in the simplified geometry will be within this distance of the original geometry)
  */
case class ST_SimplifyPreserveTopology(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val distanceTolerance = inputExpressions(1).eval(input) match {
      case number: Decimal => number.toDouble
      case number: Double => number
      case number: Int => number.toDouble
    }
    inputExpressions(0).toGeometry(input) match {
      case geometry: Geometry => TopologyPreservingSimplifier.simplify(geometry, distanceTolerance).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Reduce the precision of the given geometry to the given number of decimal places
  *
  * @param inputExpressions The first arg is a geom and the second arg is an integer scale, specifying the number of decimal places of the new coordinate. The last decimal place will
  *                         be rounded to the nearest number.
  */
case class ST_PrecisionReduce(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val precisionScale = inputExpressions(1).eval(input).asInstanceOf[Int]
    inputExpressions(0).toGeometry(input) match {
      case geometry: Geometry =>
        val precisionReduce =new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)))
        precisionReduce.reduce(geometry).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AsText(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val writer = new WKTWriter(GeometrySerializer.getDimension(geometry))
    UTF8String.fromString(writer.write(geometry))
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AsGeoJSON(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val writer = new GeoJSONWriter()
    UTF8String.fromString(writer.write(geometry).toString)
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AsBinary(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  inputExpressions.validateLength(1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val dimensions = if (geometry.isEmpty() || java.lang.Double.isNaN(geometry.getCoordinate.getZ)) 2 else 3
    val endian = if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ByteOrderValues.BIG_ENDIAN else ByteOrderValues.LITTLE_ENDIAN
    val writer = new WKBWriter(dimensions, endian)
    writer.write(geometry)
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AsEWKB(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  inputExpressions.validateLength(1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val dimensions = if (geometry.isEmpty() || java.lang.Double.isNaN(geometry.getCoordinate.getZ)) 2 else 3
    val endian = if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ByteOrderValues.BIG_ENDIAN else ByteOrderValues.LITTLE_ENDIAN
    val writer = new WKBWriter(dimensions, endian, geometry.getSRID != 0)
    writer.write(geometry)
  }

  override def dataType: DataType = BinaryType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_SRID(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  inputExpressions.validateLength(1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry.getSRID
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_SetSRID(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  inputExpressions.validateLength(2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val srid = inputExpressions(1).eval(input).asInstanceOf[Integer]
    inputExpressions(0).toGeometry(input) match {
      case geometry: Geometry =>
        val factory = new GeometryFactory(geometry.getPrecisionModel, srid, geometry.getFactory.getCoordinateSequenceFactory)
        factory.createGeometry(geometry).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_GeometryType(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    UTF8String.fromString("ST_" + geometry.getGeometryType)
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Returns a LineString formed by sewing together the constituent line work of a MULTILINESTRING.
  * Only works for MultiLineString. Using other geometry will return GEOMETRYCOLLECTION EMPTY
  * If the MultiLineString is can't be merged, the original multilinestring is returned
  *
  * @param inputExpressions Geometry
  */
case class ST_LineMerge(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  // Definition of the Geometry Collection Empty
  lazy val GeometryFactory = new GeometryFactory()
  lazy val emptyGeometry = GeometryFactory.createGeometryCollection(null)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val merger = new LineMerger()

    val output: Geometry = geometry match {
      case g: MultiLineString => {
        // Add the components of the multilinestring to the merger
        (0 until g.getNumGeometries).map(i => {
          val line = g.getGeometryN(i).asInstanceOf[LineString]
          merger.add(line)
        })
        if (merger.getMergedLineStrings().size() == 1) {
          // If the merger was able to join the lines, there will be only one element
          merger.getMergedLineStrings().iterator().next().asInstanceOf[Geometry]
        } else {
          // if the merger couldn't join the lines, it will contain the individual lines, so return the input
          geometry
        }
      }
      case _ => emptyGeometry
    }
    new GenericArrayData(GeometrySerializer.serialize(output))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_Azimuth(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    (leftGeometry, rightGeometry) match {
      case (pointA: Point, pointB: Point) => calculateAzimuth(pointA, pointB)
    }
  }

  private def calculateAzimuth(pointA: Point, pointB: Point): Double = {
    val deltaX = pointB.getX - pointA.getX
    val deltaY = pointB.getY - pointA.getY
    val azimuth = math.atan2(deltaX, deltaY)
    if (azimuth < 0) azimuth + (2 * math.Pi) else azimuth
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_X(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case point: Point => point.getX
      case _ => null
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_Y(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case point: Point => point.getY
      case _ => null
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_Z(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case point: Point => point.getCoordinate.getZ
      case _ => null
    }
  }

  override def dataType: DataType = DoubleType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_StartPoint(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case line: LineString => line.getPointN(0).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_Boundary(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val geometryBoundary = geometry.getBoundary
    geometryBoundary.toGenericArrayData
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_MinimumBoundingRadius(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  private val geometryFactory = new GeometryFactory()

  override protected def nullSafeEval(geometry: Geometry): Any = {
    getMinimumBoundingRadius(geometry)
  }

  private def getMinimumBoundingRadius(geom: Geometry): InternalRow = {
    val minimumBoundingCircle = new MinimumBoundingCircle(geom)
    val centerPoint = geometryFactory.createPoint(minimumBoundingCircle.getCentre)
    InternalRow(centerPoint.toGenericArrayData, minimumBoundingCircle.getRadius)
  }

  override def dataType: DataType = DataTypes.createStructType(
    Array(
      DataTypes.createStructField("center", GeometryUDT, false),
      DataTypes.createStructField("radius", DataTypes.DoubleType, false)
    )
  )

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_MinimumBoundingCircle(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  inputExpressions.betweenLength(1, 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val quadrantSegments = if (inputExpressions.length == 2) {
      inputExpressions(1).toInt(input)
    } else {
      BufferParameters.DEFAULT_QUADRANT_SEGMENTS
    }
    geometry match {
      case geom: Geometry => getMinimumBoundingCircle(geom, quadrantSegments).toGenericArrayData
      case _ => null
    }
  }

  private def getMinimumBoundingCircle(geom: Geometry, quadrantSegments: Int): Geometry = {
    val minimumBoundingCircle = new MinimumBoundingCircle(geom)
    val centre = minimumBoundingCircle.getCentre
    val radius = minimumBoundingCircle.getRadius
    var circle: Geometry = null
    if (centre == null) {
      circle = geom.getFactory.createPolygon
    } else {
      circle = geom.getFactory.createPoint(centre)
      if (radius != 0D)
        circle = circle.buffer(radius, quadrantSegments)
    }
    circle
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
 * Return a linestring being a substring of the input one starting and ending at the given fractions of total 2d length.
 * Second and third arguments are Double values between 0 and 1. This only works with LINESTRINGs.
 *
 * @param inputExpressions
 */
case class ST_LineSubstring(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 3)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val fractions = inputExpressions.slice(1, 3).map{
      x => x.eval(input) match {
        case a: Decimal => a.toDouble
        case a: Double => a
        case a: Int => a
      }
    }

    (geometry, fractions) match {
      case (g:LineString, r:Seq[Double]) if r.head >= 0 && r.last <= 1 && r.last >= r.head => getLineSubstring(g, r)
      case _ => null
    }
  }

  private def getLineSubstring(geom: Geometry, fractions: Seq[Double]): Any = {
    val length = geom.getLength()
    val indexedLine = new LengthIndexedLine(geom)
    val subLine = indexedLine.extractLine(length * fractions.head, length * fractions.last)
    subLine.toGenericArrayData
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a point interpolated along a line. First argument must be a LINESTRING.
 * Second argument is a Double between 0 and 1 representing fraction of
 * total linestring length the point has to be located.
 *
 * @param inputExpressions
 */
case class ST_LineInterpolatePoint(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions(0).toGeometry(input)
    val fraction: Double = inputExpressions(1).eval(input) match {
      case a: Decimal => a.toDouble
      case a: Double => a
      case a: Int => a
    }

    (geometry, fraction) match {
      case (g:LineString, f:Double) if f >= 0 && f <= 1 => getLineInterpolatePoint(g, f)
      case _ => null
    }
  }

  private def getLineInterpolatePoint(geom: Geometry, fraction: Double): Any = {
    val length = geom.getLength()
    val indexedLine = new LengthIndexedLine(geom)
    val interPoint = indexedLine.extractPoint(length * fraction)
    new GeometryFactory().createPoint(interPoint).toGenericArrayData
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_EndPoint(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case string: LineString => string.getEndPoint.toGenericArrayData
      case _ => null
    }

  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_ExteriorRing(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case polygon: Polygon => polygon.getExteriorRing.toGenericArrayData
      case _ => null
    }

  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_GeometryN(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with Logging {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions(0).toGeometry(input)
    val n = inputExpressions(1).toInt(input)
    geometry match {
      case geom: Geometry => getNthGeom(geom, n)
      case _ => null
    }
  }

  private def getNthGeom(geom: Geometry, index: Int): GenericArrayData = {
    val nthGeom = Try(geom.getGeometryN(index))
    nthGeom match {
      case Failure(exception) =>
        logWarning(s"Geometry ${geom.toString}, Out Of index")
        null
      case Success(geom) => geom.toGenericArrayData
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_InteriorRingN(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions(0).toGeometry(input)
    val n = inputExpressions(1).toInt(input)
    geometry match {
      case geom: Polygon => geom.getInteriorRingN(n)
        .toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_Dump(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    val geometryCollection = geometry match {
      case collection: GeometryCollection => {
        val numberOfGeometries = collection.getNumGeometries
        (0 until numberOfGeometries).map(
          index => collection.getGeometryN(index).toGenericArrayData
        ).toArray
      }
      case geom: Geometry => Array(geom.toGenericArrayData)
    }
    ArrayData.toArrayData(geometryCollection)
  }

  override def dataType: DataType = ArrayType(GeometryUDT)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_DumpPoints(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    ArrayData.toArrayData(geometry.getPoints.map(geom => geom.toGenericArrayData))
  }

  override def dataType: DataType = ArrayType(GeometryUDT)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_IsClosed(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case circle: Circle => true
      case point: MultiPoint => true
      case multilinestring: MultiLineString => multilinestring.isClosed
      case multipolygon: MultiPolygon => true
      case collection: GeometryCollection => false
      case string: LineString => string.isClosed
      case point: Point => true
      case polygon: Polygon => true
      case _ => null
    }
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_NumInteriorRings(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case polygon: Polygon => polygon.getNumInteriorRing
      case _: Geometry => null
    }
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AddPoint(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  inputExpressions.betweenLength(2, 3)

  private val geometryFactory = new GeometryFactory()

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val point = inputExpressions(1).toGeometry(input)
    if (inputExpressions.length == 2) addPointToGeometry(geometry, point, -1) else {
      val index = inputExpressions(2).toInt(input)
      addPointToGeometry(geometry, point, index)
    }
  }

  private def addPointToGeometry(geometry: Geometry, pointGeom: Geometry, index: Int): GenericArrayData = {
    geometry match {
      case string: LineString => pointGeom match {
        case point: Point => addPointToLineString(string, point, index) match {
          case None => null
          case Some(geom) => geom.toGenericArrayData
        }
        case _ => null
      }
      case _ => null
    }
  }

  private def addPointToLineString(lineString: LineString, point: Point, index: Int): Option[LineString] = {
    val coordinates = lineString.getCoordinates
    val length = coordinates.length
    if (index == -1) Some(lineStringFromCoordinates(coordinates ++ Array(point.getCoordinate)))
    else if (index >= 0 && index <= length) Some(lineStringFromCoordinates(
      coordinates.slice(0, index) ++ Array(point.getCoordinate) ++ coordinates.slice(index, length)))
    else None
  }

  private def lineStringFromCoordinates(coordinates: Array[Coordinate]): LineString =
    geometryFactory.createLineString(coordinates)

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_RemovePoint(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  private val geometryFactory = new GeometryFactory()

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val linesString = inputExpressions(0).toGeometry(input)
    val pointToRemove = inputExpressions(1).eval(input).asInstanceOf[Int]
    linesString match {
      case string: LineString =>
        val coordinates = string.getCoordinates
        val length = coordinates.length
        if (coordinates.length <= pointToRemove | coordinates.length <= 2) null
        else {
          val coordinatesWithPointRemoved = coordinates.slice(0, pointToRemove) ++ coordinates.slice(pointToRemove + 1, length)
          geometryFactory.createLineString(coordinatesWithPointRemoved).toGenericArrayData
        }
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_IsRing(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry match {
      case string: LineString => string.isSimple & string.isClosed
      case _ => null
    }
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Returns the number of Geometries. If geometry is a GEOMETRYCOLLECTION (or MULTI*) return the number of geometries,
  * for single geometries will return 1
  *
  * This method implements the SQL/MM specification. SQL-MM 3: 9.1.4
  *
  * @param inputExpressions Geometry
  */
case class ST_NumGeometries(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    geometry.getNumGeometries()
  }

  override def dataType: DataType = IntegerType


  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Returns a version of the given geometry with X and Y axis flipped.
  *
  * @param inputExpressions Geometry
  */
case class ST_FlipCoordinates(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    GeomUtils.flipCoordinates(geometry)
    geometry.toGenericArrayData
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_SubDivide(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    inputExpressions(0).toGeometry(input) match {
      case geom: Geometry => ArrayData.toArrayData(
        GeometrySubDivider.subDivide(geom, inputExpressions(1).toInt(input)).map(_.toGenericArrayData)
      )
      case null => null
    }

  }

  override def dataType: DataType = ArrayType(GeometryUDT)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_SubDivideExplode(children: Seq[Expression])
  extends Generator with CodegenFallback {
  children.validateLength(2)

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val geometryRaw = children.head
    val maxVerticesRaw = children(1)
    geometryRaw.toGeometry(input) match {
      case geom: Geometry => ArrayData.toArrayData(
        GeometrySubDivider.subDivide(geom, maxVerticesRaw.toInt(input)).map(_.toGenericArrayData)
      )
        GeometrySubDivider.subDivide(geom, maxVerticesRaw.toInt(input)).map(_.toGenericArrayData).map(InternalRow(_))
      case _ => new Array[InternalRow](0)
    }
  }
  override def elementSchema: StructType = {
    new StructType()
      .add("geom", GeometryUDT, true)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(children = newChildren)
  }
}


case class ST_MakePolygon(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  inputExpressions.betweenLength(1, 2)

  override def nullable: Boolean = true
  private val geometryFactory = new GeometryFactory()

  override def eval(input: InternalRow): Any = {
    val exteriorRing = inputExpressions.head
    val possibleHolesRaw = inputExpressions.tail.headOption.map(_.eval(input).asInstanceOf[ArrayData])
    val numOfElements = possibleHolesRaw.map(_.numElements()).getOrElse(0)

    val holes = (0 until numOfElements).map(el => possibleHolesRaw match {
      case Some(value) => Some(value.getArray(el))
      case None => None
    }).filter(_.nonEmpty)
      .map(el => el.map(_.toGeometry))
      .flatMap{
        case maybeLine: Option[LineString] =>
          maybeLine.map(line => geometryFactory.createLinearRing(line.getCoordinates))
        case _ => None
      }

    exteriorRing.toGeometry(input) match {
      case geom: LineString =>
        try {
          val poly = new Polygon(geometryFactory.createLinearRing(geom.getCoordinates), holes.toArray, geometryFactory)
          poly.toGenericArrayData
        }
        catch {
          case e: Exception => null
        }

      case _ => null
    }

  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_GeoHash(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions(0).toGeometry(input)

    val precision = inputExpressions(1).toInt(input)

    geometry match {
      case geom: Geometry =>
        val geoHash = GeometryGeoHashEncoder.calculate(geom, precision)
        geoHash match {
          case Some(value) => UTF8String.fromString(value)
          case None => null
        }

      case _ => null
    }

  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the difference between geometry A and B
 *
 * @param inputExpressions
 */
case class ST_Difference(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  lazy val GeometryFactory = new GeometryFactory()
  lazy val emptyPolygon = GeometryFactory.createPolygon(null, null)

  override protected def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    val isIntersects = leftGeometry.intersects(rightGeometry)
    lazy val isRightContainsLeft = rightGeometry.contains(leftGeometry)

    if (!isIntersects) {
      new GenericArrayData(GeometrySerializer.serialize(leftGeometry))
    }

    if (isIntersects && isRightContainsLeft) {
      new GenericArrayData(GeometrySerializer.serialize(emptyPolygon))
    }

    new GenericArrayData(GeometrySerializer.serialize(leftGeometry.difference(rightGeometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the symmetrical difference between geometry A and B
 *
 * @param inputExpressions
 */
case class ST_SymDifference(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override protected def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(leftGeometry.symDifference(rightGeometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the union of geometry A and B
 *
 * @param inputExpressions
 */
case class ST_Union(inputExpressions: Seq[Expression])
  extends BinaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override protected def nullSafeEval(leftGeometry: Geometry, rightGeometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(leftGeometry.union(rightGeometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_Multi(inputExpressions: Seq[Expression]) extends UnaryGeometryExpression with CodegenFallback{
  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }

  override protected def nullSafeEval(geometry: Geometry): Any ={
    Collect.createMultiGeometry(Seq(geometry)).toGenericArrayData
  }
}

/**
 * Returns a POINT guaranteed to lie on the surface.
 *
 * @param inputExpressions Geometry
 */
case class ST_PointOnSurface(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(GeomUtils.getInteriorPoint(geometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the geometry with vertex order reversed
 *
 * @param inputExpressions
 */
case class ST_Reverse(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    new GenericArrayData(GeometrySerializer.serialize(geometry.reverse()))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the geometry in EWKT format
 *
 * @param inputExpressions
 */
case class ST_AsEWKT(inputExpressions: Seq[Expression])
  extends UnaryGeometryExpression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override protected def nullSafeEval(geometry: Geometry): Any = {
    UTF8String.fromString(GeomUtils.getEWKT(geometry))
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}
