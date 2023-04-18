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

import org.apache.sedona.common.Constructors
import org.apache.sedona.common.enums.FileDataSplitter
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits.GeometryEnhancer
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Return a point from a string. The string must be plain string and each coordinate must be separated by a delimiter.
  *
  * @param inputExpressions This function takes 2 parameters. The first parameter is the input geometry
  *                         string, the second parameter is the delimiter. String format should be similar to CSV/TSV
  */
case class ST_PointFromText(inputExpressions: Seq[Expression])
  extends InferredBinaryExpression(Constructors.pointFromText) with FoldableExpression {
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
  extends InferredBinaryExpression(Constructors.polygonFromText) with FoldableExpression {
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
  extends InferredUnaryExpression(Constructors.lineFromText) with FoldableExpression {
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
  extends InferredBinaryExpression(Constructors.lineStringFromText) with FoldableExpression {
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
        Constructors.geomFromText(geomString.toString, FileDataSplitter.WKB).toGenericArrayData
      }
      case (wkb: Array[Byte]) => {
        // convert raw wkb byte array to geometry
        Constructors.geomFromWKB(wkb).toGenericArrayData
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
    val geometry = Constructors.geomFromText(geomString, FileDataSplitter.GEOJSON)
    // If the user specify a bunch of attributes to go with each geometry, we need to store all of them in this geometry
    if (inputExpressions.length > 1) {
      geometry.setUserData(generateUserData(minInputLength, inputExpressions, inputRow))
    }
    GeometrySerializer.serialize(geometry)
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
  * @param inputExpressions This function takes 2 parameter which are point x, y.
  */
case class ST_Point(inputExpressions: Seq[Expression])
  extends InferredBinaryExpression(Constructors.point) with FoldableExpression {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a Point from X, Y, Z and srid
 *
 * @param inputExpressions This function takes 4 parameter which are point x, y, z and srid (default 0).
 */
case class ST_PointZ(inputExpressions: Seq[Expression])
  extends InferredQuarternaryExpression(Constructors.pointZ) with FoldableExpression {

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
  extends InferredQuarternaryExpression(Constructors.polygonFromEnvelope) with FoldableExpression {

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
  extends InferredBinaryExpression(Constructors.geomFromGeoHash) with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
  override def allowRightNull: Boolean = true
}

case class ST_GeomFromGML(inputExpressions: Seq[Expression])
  extends InferredUnaryExpression(Constructors.geomFromGML) with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

case class ST_GeomFromKML(inputExpressions: Seq[Expression])
  extends InferredUnaryExpression(Constructors.geomFromKML) with FoldableExpression {
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

