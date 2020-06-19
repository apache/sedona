/*
 * FILE: Functions.scala
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.geosparksql.expressions

import java.util

import com.vividsolutions.jts.geom.{PrecisionModel, _}
import com.vividsolutions.jts.operation.IsSimpleOp
import com.vividsolutions.jts.operation.valid.IsValidOp
import com.vividsolutions.jts.precision.GeometryPrecisionReducer
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.datasyslab.geosparksql.utils.GeometrySerializer
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.referencing.operation.MathTransform

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
        val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
        geometry.getCoordinates.length
      case _ => None
    }
  }

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = inputExpressions

}

/**
  * Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance.
  *
  * @param inputExpressions
  */
case class ST_Buffer(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val buffer: Double = inputExpressions(1).eval(input) match {
      case a: Decimal => a.toDouble
      case a: Double => a
      case a: Int => a
    }
    new GenericArrayData(GeometrySerializer.serialize(geometry.buffer(buffer)))
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

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Given an invalid polygon or multipolygon and removeHoles boolean flag, create a valid representation of the geometry
  *
  * @param inputExpressions
  */
case class ST_MakeValid(inputExpressions: Seq[Expression])
  extends Generator with CodegenFallback with UserDataGeneratator {

  override def elementSchema: StructType = new StructType().add("Geometry", new GeometryUDT)

  override def toString: String = s" **${ST_MakeValid.getClass.getName}** "

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    assert(inputExpressions.length == 2)

    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val removeHoles = inputExpressions(1).eval(input).asInstanceOf[Boolean]

    // in order to do flatMap on java collections(util.List[Polygon])
    import scala.collection.JavaConversions._

    // makeValid works only on polygon or multipolygon
    if (!geometry.getGeometryType.equalsIgnoreCase("POLYGON") && !geometry.getGeometryType.equalsIgnoreCase("MULTIPOLYGON")) {
      throw new IllegalArgumentException("ST_MakeValid works only on Polygons and MultiPolygons")
    }
    
    val validGeometry: util.List[Polygon] = geometry match {
      case g: MultiPolygon =>
        (0 until g.getNumGeometries).flatMap(i => {
          val polygon = g.getGeometryN(i).asInstanceOf[Polygon]
          val fixedPolygons = JTS.makeValid(polygon, removeHoles)
          fixedPolygons
        })
      case g: Polygon =>
        JTS.makeValid(g, removeHoles)
      case _ => Nil
    }

    val result = validGeometry.toArray.map(g => {
      val serializedGeometry = GeometrySerializer.serialize(g.asInstanceOf[Geometry])
      InternalRow(new GenericArrayData(serializedGeometry))
    })

    result
  }

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
  * Test if Geometry is simple.
  *
  * @param inputExpressions
  */
case class ST_IsSimple(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)

    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])

    val isSimpleop = new IsSimpleOp(geometry)

    return isSimpleop.isSimple
  }

  override def dataType: DataType = BooleanType

  override def children: Seq[Expression] = inputExpressions
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
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val distanceTolerance = inputExpressions(1).eval(input) match {
      case number: Decimal => number.toDouble
      case number: Double => number
      case number: Int => number.toDouble
    }
    val simplifiedGeometry = TopologyPreservingSimplifier.simplify(geometry, distanceTolerance)

    new GenericArrayData(GeometrySerializer.serialize(simplifiedGeometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
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
    assert(inputExpressions.length == 2)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    val precisionScale = inputExpressions(1).eval(input).asInstanceOf[Int]
    val precisionReduce = new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)))
    new GenericArrayData(GeometrySerializer.serialize(precisionReduce.reduce(geometry)))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

case class ST_AsText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    UTF8String.fromString(geometry.toText)
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions
}

case class ST_GeometryType(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback{
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])
    UTF8String.fromString("ST_" + geometry.getGeometryType)
  }

  override def dataType: DataType = StringType

  override def children: Seq[Expression] = inputExpressions
}


/**
 * Returns the first point of a LINESTRING geometry as a POINT or NULL if the input parameter is not a LINESTRING.
 * This method implements the SQL/MM specification. SQL-MM 3: 7.1.3
 *
 * @param inputExpressions A geometry
 */
case class ST_StartPoint(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    assert(inputExpressions.length == 1)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(input).asInstanceOf[ArrayData])

    if (geometry.getGeometryType.equalsIgnoreCase("LINESTRING")) {
      new GenericArrayData(GeometrySerializer.serialize(geometry.asInstanceOf[LineString].getStartPoint()))
    } else {
      null
    }
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}