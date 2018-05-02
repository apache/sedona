/*
 * FILE: Constructors.scala
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

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String
import org.datasyslab.geospark.enums.{FileDataSplitter, GeometryType}
import org.datasyslab.geospark.formatMapper.FormatMapper
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geosparksql.utils.GeometrySerializer

/**
  * Return a point from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions This function takes 2 parameters. The first parameter is the input geometry
  *                         string, the second parameter is the delimiter. String format should be similar to CSV/TSV
  */
case class ST_PointFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes two input expressions.
    val minInputLength = 2
    assert(inputExpressions.length >= minInputLength)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POINT)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > minInputLength) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }

    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a polygon from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_PolygonFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes two input expressions.
    val minInputLength = 2
    assert(inputExpressions.length >= minInputLength)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POLYGON)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > minInputLength) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }

    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a linestring from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_LineStringFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes two input expressions.
    val minInputLength = 2
    assert(inputExpressions.length >= minInputLength)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.LINESTRING)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > minInputLength) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }

    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a Geometry from a WKT string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be WKT.
  */
case class ST_GeomFromWKT(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    val minInputLength = 1
    assert(inputExpressions.length >= minInputLength)

    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.WKT
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > minInputLength) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a Geometry from a WKB string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be WKB.
  */
case class ST_GeomFromWKB(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    val minInputLength = 1
    assert(inputExpressions.length >= minInputLength)

    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.WKB
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > minInputLength) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a Geometry from a GeoJSON string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be GeoJson.
  */
case class ST_GeomFromGeoJSON(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    val minInputLength = 1
    assert(inputExpressions.length >= minInputLength)

    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.GEOJSON
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > 1) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a Point from X and Y
  *
  * @param inputExpressions This function takes 2 parameter which are point x and y.
  */
case class ST_Point(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val minInputLength = 2
    assert(inputExpressions.length >= minInputLength)

    val x = inputExpressions(0).eval(inputRow).asInstanceOf[Decimal].toDouble
    val y = inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble
    var geometryFactory = new GeometryFactory()
    var geometry = geometryFactory.createPoint(new Coordinate(x, y))
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > minInputLength) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a Circle from a Geometry and a radius
  *
  * @param inputExpressions This function takes two parameters, a geometry column and a radius, and outputs a circle type
  */
case class ST_Circle(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    assert(inputExpressions.length == 2)
    val geometry = GeometrySerializer.deserialize(inputExpressions(0).eval(inputRow).asInstanceOf[ArrayData])
    val circle = new Circle(geometry, inputExpressions(1).eval(inputRow).asInstanceOf[Decimal].toDouble)
    return new GenericArrayData(GeometrySerializer.serialize(circle))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

/**
  * Return a polygon given minX,minY,maxX,maxY
  *
  * @param inputExpressions
  */
case class ST_PolygonFromEnvelope(inputExpressions: Seq[Expression]) extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val minInputLength = 4
    assert(inputExpressions.length >= minInputLength)
    val minX = inputExpressions(0).eval(input).asInstanceOf[Decimal].toDouble
    val minY = inputExpressions(1).eval(input).asInstanceOf[Decimal].toDouble
    val maxX = inputExpressions(2).eval(input).asInstanceOf[Decimal].toDouble
    val maxY = inputExpressions(3).eval(input).asInstanceOf[Decimal].toDouble
    var coordinates = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    val polygon = geometryFactory.createPolygon(coordinates)
    if (inputExpressions.length > minInputLength) {
      polygon.setUserData(generateUserData(minInputLength, inputExpressions, input))
    }
    new GenericArrayData(GeometrySerializer.serialize(polygon))
  }

  override def dataType: DataType = new GeometryUDT()

  override def children: Seq[Expression] = inputExpressions
}

trait UserDataGeneratator {
  def generateUserData(minInputLength: Integer, inputExpressions: Seq[Expression], inputRow: InternalRow): String = {
    var userData = inputExpressions(minInputLength).eval(inputRow).asInstanceOf[UTF8String].toString

    for (i <- minInputLength + 1 to inputExpressions.length - 1) {
      userData = userData + "\t" + inputExpressions(i).eval(inputRow).asInstanceOf[UTF8String].toString
    }
    return userData
  }
}

