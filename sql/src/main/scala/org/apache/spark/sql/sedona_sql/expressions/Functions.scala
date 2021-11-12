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
import org.apache.spark.sql.sedona_sql.expressions.geohash.{GeoHashDecoder, GeometryGeoHashEncoder, InvalidGeoHashException}
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.sedona_sql.expressions.subdivide.GeometrySubDivider
import org.apache.spark.sql.types.{ArrayType, _}
import org.apache.spark.unsafe.types.UTF8String
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.algorithm.MinimumBoundingCircle
import org.locationtech.jts.geom.{PrecisionModel, _}
import org.locationtech.jts.io.{ByteOrderValues, WKBWriter}
import org.locationtech.jts.linearref.LengthIndexedLine
import org.locationtech.jts.operation.IsSimpleOp
import org.locationtech.jts.operation.buffer.BufferParameters
import org.locationtech.jts.operation.linemerge.LineMerger
import org.locationtech.jts.operation.valid.IsValidOp
import org.locationtech.jts.precision.GeometryPrecisionReducer
import org.locationtech.jts.simplify.TopologyPreservingSimplifier
import org.opengis.referencing.operation.MathTransform
import org.wololo.jts2geojson.GeoJSONWriter

import java.nio.ByteOrder
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Return the distance between two geometries.
  *
  * @param inputExpressions This function takes two geometries and calculates the distance between two objects.
  */
case class ST_Distance(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def toString: String = s" **${ST_Distance.getClass.getName}**  "

  override def eval(inputRow: InternalRow): Any = {
    val leftArray = inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData]
    val rightArray = inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData]

    val leftGeometry = GeometrySerializer.deserialize(leftArray)

    val rightGeometry = GeometrySerializer.deserialize(rightArray)

    leftGeometry.distance(rightGeometry)
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
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
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    inputExpressions.length match {
      case 1 =>
        val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
        geometry.getCoordinates.length
      case _ => None
    }
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

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val buffer: Double = inputExpressions(1).eval(input) match {
      case a: Decimal => a.toDouble
      case a: Double => a
      case a: Int => a
    }
    new GenericArrayData(GeometrySerializer.serialize(geometry.buffer(buffer)))
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
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

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val originalGeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val sourceCRScode = CRS.decode(inputExpressions(1).eval(input).asInstanceOf[UTF8String].toString)
    val targetCRScode = CRS.decode(inputExpressions(2).eval(input).asInstanceOf[UTF8String].toString)

    var transform: MathTransform = null
    if (inputExpressions.length == 4) {
      transform = CRS.findMathTransform(sourceCRScode, targetCRScode, inputExpressions(3).eval(input).asInstanceOf[Boolean])
    }
    else {
      transform = CRS.findMathTransform(sourceCRScode, targetCRScode, false)
    }
    val geom = JTS.transform(originalGeometry, transform)
    new GenericArrayData(GeometrySerializer.serialize(geom))
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)

  lazy val GeometryFactory = new GeometryFactory()
  lazy val emptyPolygon = GeometryFactory.createPolygon(null, null)

  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val leftgeometry = GeometrySerializer.deserialize(inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData])
    val rightgeometry = GeometrySerializer.deserialize(inputExpressions(1).eval(inputRow).asInstanceOf[ArrayData])

    val isIntersects = leftgeometry.intersects(rightgeometry)
    lazy val isLeftContainsRight = leftgeometry.contains(rightgeometry)
    lazy val isRightContainsLeft = rightgeometry.contains(leftgeometry)

    if (!isIntersects) {
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

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Given an invalid polygon or multipolygon and removeHoles boolean flag, create a valid representation of the geometry
  *
  * @param inputExpressions
  */
case class ST_MakeValid(inputExpressions: Seq[Expression])
  extends Generator with CodegenFallback with UserDataGeneratator {
  assert(inputExpressions.length == 2)

  override def elementSchema: StructType = new StructType().add("Geometry", new GeometryUDT)

  override def toString: String = s" **${ST_MakeValid.getClass.getName}** "

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val removeHoles = inputExpressions(1).eval(input).asInstanceOf[Boolean]

    // in order to do flatMap on java collections(util.List[Polygon])
    import scala.jdk.CollectionConverters._

    // makeValid works only on polygon or multipolygon
    if (!geometry.getGeometryType.equalsIgnoreCase("POLYGON") && !geometry.getGeometryType.equalsIgnoreCase("MULTIPOLYGON")) {
      throw new IllegalArgumentException("ST_MakeValid works only on Polygons and MultiPolygons")
    }

    val validGeometry = geometry match {
      case g: MultiPolygon =>
        (0 until g.getNumGeometries).flatMap(i => {
          val polygon = g.getGeometryN(i).asInstanceOf[Polygon]
          JTS.makeValid(polygon, removeHoles).asScala.iterator
        })
      case g: Polygon =>
        JTS.makeValid(g, removeHoles).asScala.iterator
      case _ => Nil
    }

    val result = validGeometry.map(g => {
      val serializedGeometry = GeometrySerializer.serialize(g.asInstanceOf[Geometry])
      InternalRow(new GenericArrayData(serializedGeometry))
    })

    result
  }

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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.eval(input).asInstanceOf[ArrayData]
    if (geometry == null) {
      null
    } else {
      val isvalidop = new IsValidOp(GeometrySerializer.deserialize(geometry))
      isvalidop.isValid
    }
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
    val isSimpleop = new IsSimpleOp(geometry)
    isSimpleop.isSimple
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

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val distanceTolerance = inputExpressions(1).eval(input) match {
      case number: Decimal => number.toDouble
      case number: Double => number
      case number: Int => number.toDouble
    }
    val simplifiedGeometry = TopologyPreservingSimplifier.simplify(geometry, distanceTolerance)

    new GenericArrayData(GeometrySerializer.serialize(simplifiedGeometry))
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
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val precisionScale = inputExpressions(1).eval(input).asInstanceOf[Int]
    val precisionReduce = new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)))
    new GenericArrayData(GeometrySerializer.serialize(precisionReduce.reduce(geometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AsText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
    UTF8String.fromString(geometry.toText)
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_AsGeoJSON(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)

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
  extends Expression with CodegenFallback {
  inputExpressions.validateLength(1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val dimensions = if (java.lang.Double.isNaN(geometry.getCoordinate.getZ)) 2 else 3
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
  extends Expression with CodegenFallback {
  inputExpressions.validateLength(1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val dimensions = if (java.lang.Double.isNaN(geometry.getCoordinate.getZ)) 2 else 3
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
  extends Expression with CodegenFallback {
  inputExpressions.validateLength(1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    val srid = inputExpressions(1).eval(input).asInstanceOf[Integer]
    val factory = new GeometryFactory(geometry.getPrecisionModel, srid, geometry.getFactory.getCoordinateSequenceFactory)
    new GenericArrayData(GeometrySerializer.serialize(factory.createGeometry(geometry)))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_GeometryType(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  // Definition of the Geometry Collection Empty
  lazy val GeometryFactory = new GeometryFactory()
  lazy val emptyGeometry = GeometryFactory.createGeometryCollection(null)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])

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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 2)
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometries = (inputExpressions(0).toGeometry(input), inputExpressions(1).toGeometry(input))
    geometries match {
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)

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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)

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

case class ST_StartPoint(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  private val geometryFactory = new GeometryFactory()

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    geometry match {
      case geom: Geometry => getMinimumBoundingRadius(geom)
      case _ => null
    }
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
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
    ArrayData.toArrayData(geometry.getPoints.map(geom => geom.toGenericArrayData))
  }

  override def dataType: DataType = ArrayType(GeometryUDT)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


case class ST_IsClosed(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geometry = inputExpressions.head.toGeometry(input)
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions.head.eval(input).asInstanceOf[ArrayData])
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
  extends Expression with CodegenFallback {
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
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
