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
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, Decimal, DoubleType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory, Polygon}
import org.apache.raster.Construction
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

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
    assert(inputExpressions.length == 2)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POINT)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

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
    assert(inputExpressions.length == 2)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.POLYGON)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

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
    assert(inputExpressions.length == 2)

    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val geomFormat = inputExpressions(1).eval(inputRow).asInstanceOf[UTF8String].toString

    var fileDataSplitter = FileDataSplitter.getFileDataSplitter(geomFormat)
    var formatMapper = new FormatMapper(fileDataSplitter, false, GeometryType.LINESTRING)
    var geometry = formatMapper.readGeometry(geomString)

    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

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
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.WKT
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions
}


/**
  * Return a Geometry from a WKT string
  *
  * @param inputExpressions This function takes 1 parameter which is the geometry string. The string format must be WKT.
  */
case class ST_GeomFromText(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.WKT
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

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
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    var fileDataSplitter = FileDataSplitter.WKB
    var formatMapper = new FormatMapper(fileDataSplitter, false)
    var geometry = formatMapper.readGeometry(geomString)
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

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

  override def dataType: DataType = GeometryUDT

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
    assert(inputExpressions.length == 2)
    val x = inputExpressions(0).eval(inputRow) match {
      case a: Double => a
      case b: Decimal => b.toDouble
    }
    val y = inputExpressions(1).eval(inputRow) match {
      case a: Double => a
      case b: Decimal => b.toDouble
    }

    var geometryFactory = new GeometryFactory()
    var geometry = geometryFactory.createPoint(new Coordinate(x, y))
    return new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

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
    assert(inputExpressions.length == 4)

    val minX = inputExpressions(0).eval(input) match {
      case a: Double => a
      case b: Decimal => b.toDouble
    }

    val minY = inputExpressions(1).eval(input) match {
      case a: Double => a
      case b: Decimal => b.toDouble
    }

    val maxX = inputExpressions(2).eval(input) match {
      case a: Double => a
      case b: Decimal => b.toDouble
    }

    val maxY = inputExpressions(3).eval(input) match {
      case a: Double => a
      case b: Decimal => b.toDouble
    }

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

case class ST_GeomFromRaster(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 1)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val fileDataSplitter = FileDataSplitter.RASTER
    val formatMapper = new FormatMapper(fileDataSplitter, false)
    val geometry = formatMapper.readGeometry(geomString)
    new GenericArrayData(GeometrySerializer.serialize(geometry))
  }

  override def dataType: DataType = GeometryUDT

  override def children: Seq[Expression] = inputExpressions
}



/**
 *  Converting a spark dataframe into GeoTiff Dataframe which involves fetching geometrical extent and band values for an image(Geotiff)
 *
 * @param inputExpressions Image URL(String), Number of Bands(Integer)
 */
case class ST_DataframeFromRaster(inputExpressions: Seq[Expression])
  extends Expression with CodegenFallback with UserDataGeneratator {
  override def nullable: Boolean = false

  private var bandInfo = 0
  override def eval(inputRow: InternalRow): Any = {
    // This is an expression which takes one input expressions
    assert(inputExpressions.length == 2)
    val geomString = inputExpressions(0).eval(inputRow).asInstanceOf[UTF8String].toString
    val totalBands = inputExpressions(1).eval(inputRow).asInstanceOf[Int]
    bandInfo = totalBands
    val fileDataSplitter = FileDataSplitter.RASTER
    val formatMapper = new FormatMapper(fileDataSplitter, false)
    val const = new Construction(totalBands)
    val geometry = formatMapper.readGeometry(geomString)
    val bandvalues = const.getBands(geomString).map(e=>e.map(t=>Double2double(t))).map(arr=>arr.toArray).toArray

    returnValue(geometry.toGenericArrayData,bandvalues, totalBands)

  }

  // Dynamic results based on number of columns and type of structure
  private def returnValue(geometry:GenericArrayData, bands:Array[Array[Double]], count:Int): InternalRow = {

    val genData = new Array[GenericArrayData](count + 1)
    genData(0) = geometry
    var i = 1
    for(i <- 1 until count + 1 ) {
      genData(i) = new GenericArrayData(bands(i-1))
    }
    val result = InternalRow(genData.toList : _*)
    result
  }

  // Dynamic Schema generation using Number of Bands
  private def getSchema(count:Int):DataType = {
    var schema = Seq[String]()
    schema = schema :+ "Polygon"
    var i = 1
    for(i <- 1 until 5 ) {
     schema = schema :+ "band".concat(i.toString)
    }
    val mySchema = StructType(schema.map(n => if (n == "Polygon") StructField("Polygon", GeometryUDT, false) else StructField(n, ArrayType(DoubleType), false)))
    mySchema
  }

  override def dataType: DataType = getSchema(bandInfo)

  override def children: Seq[Expression] = inputExpressions
}
