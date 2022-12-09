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

import org.apache.sedona.core.enums.{FileDataSplitter, GeometryType}
import org.apache.sedona.core.formatMapper.FormatMapper
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.geohash.GeoHashDecoder
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer
import org.apache.spark.sql.types.{AbstractDataType, DataType, DoubleType, IntegerType, StringType, TypeCollection}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.locationtech.jts.io.WKBReader
import org.locationtech.jts.io.gml2.GMLReader
import org.locationtech.jts.io.kml.KMLReader
import org.apache.spark.sql.catalyst.expressions.ImplicitCastInputTypes
import org.apache.sedona.common.Constructors
import org.apache.spark.sql.types.BinaryType

/**
  * Return a point from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions This function takes 2 parameters. The first parameter is the input geometry
  *                         string, the second parameter is the delimiter. String format should be similar to CSV/TSV
  */
case class ST_PointFromText(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {
  // This is an expression which takes two input expressions.
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POINT)
    var geometry = formatMapper.readGeometry(geomString)
    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return a polygon from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_PolygonFromText(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {
  // This is an expression which takes two input expressions.
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POLYGON)
    var geometry = formatMapper.readGeometry(geomString)
    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return a line from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_LineFromText(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {
  // This is an expression which takes one input expressions.
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    val lineString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.WKT
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(lineString)
    if(geometry.getGeometryType.contains("LineString")) {
      new GenericArrayData(GeometrySerializer.serialize(geometry))
    } else {
      null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return a linestring from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions
  */
case class ST_LineStringFromText(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {
  // This is an expression which takes two input expressions.
  assert(inputExpressions.length == 2)

  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.LINESTRING)
    var geometry = formatMapper.readGeometry(geomString)

    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Return a Geometry from a WKT string
  *
  * @param inputExpressions This function takes a geometry string and a srid. The string format must be WKT.
  */
case class ST_GeomFromWKT(inputExpressions: Seq[Expression])
  extends InferredBinaryExpression(Constructors.geomFromWKT) with FoldableExpression {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Return a Geometry from a WKT string
  *
  * @param inputExpressions This function takes a geometry string and a srid. The string format must be WKT.
  */
case class ST_GeomFromText(inputExpressions: Seq[Expression])
  extends InferredBinaryExpression(Constructors.geomFromWKT) with FoldableExpression {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Return a Geometry from a WKB string
  *
  * @param inputExpressions This function takes 1 parameter which is the utf-8 encoded geometry wkb string or the binary wkb array.
  */
case class ST_GeomFromWKB(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {
  // This is an expression which takes one input expressions
  assert(inputExpressions.length == 1)

  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    (inputExpressions.head.eval(inputRow)) match {
      case (geomString: UTF8String) => {
        // Parse UTF-8 encoded wkb string
        val fileDataSplitter = FileDataSplitter.WKB
        val formatMapper = new FormatMapper(fileDataSplitter, false)
        formatMapper.readGeometry(geomString.toString).toGenericArrayData
      }
      case (wkb: Array[Byte]) => {
        // convert raw wkb byte array to geometry
        new WKBReader().read(wkb).toGenericArrayData
      }
      case null => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(StringType, BinaryType))

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return a Geometry from a GeoJSON string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be GeoJson.
  */
case class ST_GeomFromGeoJSON(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with CodegenFallback with UserDataGeneratator {
  // This is an expression which takes one input expressions
  val minInputLength = 1
  assert(inputExpressions.length >= minInputLength)

  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val geomString = inputExpressions.head.eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.GEOJSON
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > 1) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }
    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
  * Return a Point from X and Y
  *
  * @param inputExpressions This function takes 3 parameter which are point x, y and z.
  */
case class ST_Point(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {

  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    val x = inputExpressions(0).eval(inputRow).asInstanceOf[Double]
    val y = inputExpressions(1).eval(inputRow).asInstanceOf[Double]
    val coord = inputExpressions(2).eval(inputRow) match {
      case null => new Coordinate(x, y)
      case z: Double => new Coordinate(x, y, z)
    }
    val geometryFactory = new GeometryFactory()
    val geometry = geometryFactory.createPoint(coord)
    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType, DoubleType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}


/**
  * Return a polygon given minX,minY,maxX,maxY
  *
  * @param inputExpressions
  */
case class ST_PolygonFromEnvelope(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback with UserDataGeneratator {
  assert(inputExpressions.length == 4)

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val minX = inputExpressions(0).eval(input).asInstanceOf[Double]
    val minY = inputExpressions(1).eval(input).asInstanceOf[Double]
    val maxX = inputExpressions(2).eval(input).asInstanceOf[Double]
    val maxY = inputExpressions(3).eval(input).asInstanceOf[Double]
    var coordinates = new Array[Coordinate](5)
    coordinates(0) = new Coordinate(minX, minY)
    coordinates(1) = new Coordinate(minX, maxY)
    coordinates(2) = new Coordinate(maxX, maxY)
    coordinates(3) = new Coordinate(maxX, minY)
    coordinates(4) = coordinates(0)
    val geometryFactory = new GeometryFactory()
    val polygon = geometryFactory.createPolygon(coordinates)
    new GenericArrayData(GeometrySerializer.serialize(polygon))
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType, DoubleType, DoubleType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

trait UserDataGeneratator {
  def generateUserData(minInputLength: Integer, inputExpressions: Seq[Expression], inputRow: InternalRow): String = {
    var userData = inputExpressions(minInputLength).eval(inputRow).asInstanceOf[UTF8String].toString

    for (i <- minInputLength + 1 until inputExpressions.length) {
      userData = userData + "\t" + inputExpressions(i).eval(inputRow).asInstanceOf[UTF8String].toString
    }
    userData
  }
}


case class ST_GeomFromGeoHash(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback {
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val geoHash = Option(inputExpressions.head.eval(input))
      .map(_.asInstanceOf[UTF8String].toString)
    val precision = Option(inputExpressions(1).eval(input)).map(_.asInstanceOf[Int])

    try {
      geoHash match {
        case Some(value) => GeoHashDecoder.decode(value, precision).toGenericArrayData
        case None => null
      }
    }
    catch {
      case e: Exception => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, IntegerType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_GeomFromGML(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback {
  assert(inputExpressions.length == 1)
  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    (inputExpressions(0).eval(inputRow)) match {
      case geomString: UTF8String =>
        new GMLReader().read(geomString.toString, new GeometryFactory()).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_GeomFromKML(inputExpressions: Seq[Expression])
  extends Expression with FoldableExpression with ImplicitCastInputTypes with CodegenFallback {
  assert(inputExpressions.length == 1)
  override def nullable: Boolean = true

  override def eval(inputRow: InternalRow): Any = {
    inputExpressions(0).eval(inputRow) match {
      case geomString: UTF8String =>
        new KMLReader().read(geomString.toString).toGenericArrayData
      case _ => null
    }
  }

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKT string
 *
 * @param inputExpressions This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_MPolyFromText(inputExpressions: Seq[Expression])
  extends InferredBinaryExpression(Constructors.mPolyFromText) with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Geometry from a WKT string
 *
 * @param inputExpressions This function takes a geometry string and a srid. The string format must be WKT.
 */
case class ST_MLineFromText(inputExpressions: Seq[Expression])
  extends InferredBinaryExpression(Constructors.mLineFromText) with FoldableExpression {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

