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

import org.apache.sedona.common.{Functions, FunctionsGeoTools, FunctionsProj4}
import org.apache.sedona.common.sphere.{Haversine, Spheroid}
import org.apache.sedona.common.utils.{InscribedCircle, ValidDetail}
import org.apache.sedona.core.utils.SedonaConf
import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, Generator, ImplicitCastInputTypes, Nondeterministic, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.sedona_sql.expressions.implicits._
import org.apache.spark.sql.types._
import org.locationtech.jts.algorithm.MinimumBoundingCircle
import org.locationtech.jts.geom._
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.InferrableFunctionConverter._
import org.apache.spark.sql.sedona_sql.expressions.LibPostalUtils.{getExpanderFromConf, getParserFromConf}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import com.mapzen.jpostal.{AddressExpander, AddressParser}
import org.apache.sedona.common.S2Geography.Geography
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper

private[apache] case class ST_LabelPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(Functions.labelPoint),
      inferrableFunction2(Functions.labelPoint),
      inferrableFunction3(Functions.labelPoint)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the distance between two geometries.
 *
 * @param inputExpressions
 *   This function takes two geometries and calculates the distance between two objects.
 */
private[apache] case class ST_Distance(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.distance _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_YMax(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.yMax _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_YMin(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.yMin _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the Z maxima of the geometry.
 *
 * @param inputExpressions
 *   This function takes a geometry and returns the maximum of all Z-coordinate values.
 */
private[apache] case class ST_ZMax(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.zMax _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the Z minima of the geometry.
 *
 * @param inputExpressions
 *   This function takes a geometry and returns the minimum of all Z-coordinate values.
 */
private[apache] case class ST_ZMin(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.zMin _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_3DDistance(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.distance3d _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the concave hull of a Geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_ConcaveHull(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.concaveHull _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the convex hull of a Geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_ConvexHull(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.convexHull _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_CrossesDateLine(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.crossesDateLine _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the number of Points in geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_NPoints(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.nPoints _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the number of Dimensions in geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_NDims(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.nDims _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a geometry/geography that represents all points whose distance from this
 * Geometry/geography is less than or equal to distance.
 *
 * @param inputExpressions
 */
private[apache] case class ST_Buffer(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.buffer),
      inferrableFunction3(Functions.buffer),
      inferrableFunction4(Functions.buffer)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_BestSRID(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.bestSRID _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ShiftLongitude(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.shiftLongitude _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the bounding rectangle for a Geometry
 *
 * @param inputExpressions
 */
private[apache] case class ST_Envelope(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(Functions.envelope),
      inferrableFunction2(org.apache.sedona.common.geography.Functions.getEnvelope)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Expand(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction4(Functions.expand),
      inferrableFunction3(Functions.expand),
      inferrableFunction2(Functions.expand)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the length measurement of a Geometry
 *
 * @param inputExpressions
 */
private[apache] case class ST_Length(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.length _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the length measurement of a Geometry
 *
 * @param inputExpressions
 */
private[apache] case class ST_Length2D(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.length _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the area measurement of a Geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_Area(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.area _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return mathematical centroid of a geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_Centroid(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.getCentroid _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Given a geometry, sourceEPSGcode, and targetEPSGcode, convert the geometry's Spatial Reference
 * System / Coordinate Reference System.
 *
 * The CRS transformation backend is controlled by the `spark.sedona.crs.geotools` config:
 *   - `none`: Use proj4sedona for all vector transformations
 *   - `raster` (default): Use proj4sedona for vector, GeoTools for raster
 *   - `all`: Use GeoTools for everything (legacy behavior)
 *
 * Note: For the 4-argument version with lenient parameter, proj4sedona ignores the lenient
 * parameter (it always performs strict transformation). GeoTools uses the lenient parameter.
 *
 * The default fallback (when config cannot be read) is proj4sedona, ensuring consistent behavior
 * during Spark's query optimization phases like constant folding.
 *
 * @param inputExpressions
 * @param useGeoTools
 */
private[apache] case class ST_Transform(
    inputExpressions: Seq[Expression],
    useGeoTools: Boolean,
    crsUrlBase: String,
    crsUrlPathTemplate: String,
    crsUrlFormat: String)
    extends InferredExpression(
      inferrableFunction4(FunctionsProj4.transform),
      inferrableFunction3(FunctionsProj4.transform),
      inferrableFunction2(FunctionsProj4.transform)) {

  private def this(
      inputExpressions: Seq[Expression],
      config: (Boolean, String, String, String)) = {
    this(inputExpressions, config._1, config._2, config._3, config._4)
  }

  def this(inputExpressions: Seq[Expression]) = {
    // Read all config from SedonaConf on the driver and pass to primary constructor.
    // SparkSession may not be available on executors, so config is captured here
    // and serialized to executors along with the expression node.
    this(inputExpressions, ST_Transform.readConfig())
  }

  // Define proj4sedona function overloads (2, 3, 4-arg versions)
  // Note: 4-arg version ignores the lenient parameter
  private lazy val proj4Functions: Seq[InferrableFunction] = Seq(
    inferrableFunction4(FunctionsProj4.transform),
    inferrableFunction3(FunctionsProj4.transform),
    inferrableFunction2(FunctionsProj4.transform))

  // Define GeoTools function overloads (2, 3, 4-arg versions)
  private lazy val geoToolsFunctions: Seq[InferrableFunction] = Seq(
    inferrableFunction4(FunctionsGeoTools.transform),
    inferrableFunction3(FunctionsGeoTools.transform),
    inferrableFunction2(FunctionsGeoTools.transform))

  override lazy val f: InferrableFunction = {
    // Register URL CRS provider on executor if configured (lazy, once per JVM).
    // This runs inside lazy val f so it only executes on executors during row
    // evaluation, never on the driver during query planning.
    if (crsUrlBase.nonEmpty) {
      FunctionsProj4.registerUrlCrsProvider(crsUrlBase, crsUrlPathTemplate, crsUrlFormat)
    }

    // Check config to decide between proj4sedona and GeoTools
    // Note: 4-arg lenient parameter is ignored by proj4sedona
    val candidateFunctions = if (useGeoTools) geoToolsFunctions else proj4Functions
    FunctionResolver.resolveFunction(inputExpressions, candidateFunctions)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

object ST_Transform {

  /**
   * Read all ST_Transform config from SedonaConf in one call. Defaults are handled by SedonaConf
   * itself. Returns safe fallbacks (proj4sedona, no URL provider) when no active session exists.
   */
  private def readConfig(): (Boolean, String, String, String) = {
    try {
      val conf = SedonaConf.fromActiveSession()
      (
        conf.getCRSTransformMode.useGeoToolsForVector(),
        conf.getCrsUrlBase,
        conf.getCrsUrlPathTemplate,
        conf.getCrsUrlFormat)
    } catch {
      case _: Exception =>
        // No active session (e.g., during constant folding) — use safe defaults
        (false, "", "", "")
    }
  }
}

/**
 * Return the intersection shape of two geometries. The return type is a geometry
 *
 * @param inputExpressions
 */
private[apache] case class ST_Intersection(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.intersection _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Given an invalid geometry, create a valid representation of the geometry. See:
 * http://lin-ear-th-inking.blogspot.com/2021/05/fixing-invalid-geometry-with-jts.html
 *
 * @param inputExpressions
 */
private[apache] case class ST_MakeValid(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.makeValid _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_IsValidDetail(children: Seq[Expression])
    extends Expression
    with ExpectsInputTypes
    with CodegenFallback {

  private val nArgs = children.length

  override def inputTypes: Seq[AbstractDataType] = {
    if (nArgs == 2) {
      Seq(GeometryUDT(), IntegerType)
    } else if (nArgs == 1) {
      Seq(GeometryUDT())
    } else {
      throw new IllegalArgumentException(s"Invalid number of arguments: $nArgs")
    }
  }

  override def eval(input: InternalRow): Any = {
    val geometry = children.head.toGeometry(input)
    var validDetail: ValidDetail = null
    if (nArgs == 1) {
      validDetail = Functions.isValidDetail(geometry)
    } else if (nArgs == 2) {
      val flag = children(1).eval(input).asInstanceOf[Int]
      validDetail = Functions.isValidDetail(geometry, flag)
    } else {
      throw new IllegalArgumentException(s"Invalid number of arguments: $nArgs")
    }

    try {
      if (validDetail.location == null) {
        return InternalRow.fromSeq(Seq(validDetail.valid, null, null))
      }

      val serLocation = GeometrySerializer.serialize(validDetail.location)
      InternalRow.fromSeq(
        Seq(validDetail.valid, UTF8String.fromString(validDetail.reason), serLocation))
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(geometry),
          e)
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = new StructType()
    .add("valid", BooleanType, nullable = false)
    .add("reason", StringType, nullable = true)
    .add("location", GeometryUDT(), nullable = true)
}

private[apache] case class ST_IsValidTrajectory(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction1(Functions.isValidTrajectory)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if Geometry is valid.
 *
 * @param inputExpressions
 */
private[apache] case class ST_IsValid(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.isValid),
      inferrableFunction1(Functions.isValid)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if Geometry is simple.
 *
 * @param inputExpressions
 */
private[apache] case class ST_IsSimple(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.isSimple _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Simplifies a geometry and ensures that the result is a valid geometry having the same dimension
 * and number of components as the input, and with the components having the same topological
 * relationship. The simplification uses a maximum-distance difference algorithm similar to the
 * Douglas-Peucker algorithm.
 *
 * @param inputExpressions
 *   first arg is geometry second arg is distance tolerance for the simplification(all vertices in
 *   the simplified geometry will be within this distance of the original geometry)
 */
private[apache] case class ST_SimplifyPreserveTopology(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.simplifyPreserveTopology _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Reduce the precision of the given geometry to the given number of decimal places
 *
 * @param inputExpressions
 *   The first arg is a geom and the second arg is an integer scale, specifying the number of
 *   decimal places of the new coordinate. The last decimal place will be rounded to the nearest
 *   number.
 */
private[apache] case class ST_ReducePrecision(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.reducePrecision _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Simplify(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.simplify _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SimplifyVW(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.simplifyVW _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SimplifyPolygonHull(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.simplifyPolygonHull),
      inferrableFunction3(Functions.simplifyPolygonHull)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsText(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.asWKT _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsGeoJSON(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction1(Functions.asGeoJson),
      inferrableFunction2(Functions.asGeoJson)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsBinary(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.asWKB _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsEWKB(inputExpressions: Seq[Expression])
    extends InferredExpression((geom: Geometry) => Functions.asEWKB(geom)) {
  // (geog: Geography) => Functions.asEWKB(geog)
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsHEXEWKB(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.asHexEWKB),
      inferrableFunction1(Functions.asHexEWKB)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SRID(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.getSRID _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SetSRID(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.setSRID _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_GeometryType(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.geometryType _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a LineString formed by sewing together the constituent line work of a MULTILINESTRING.
 * Only works for MultiLineString. Using other geometry will return GEOMETRYCOLLECTION EMPTY If
 * the MultiLineString is can't be merged, the original multilinestring is returned
 *
 * @param inputExpressions
 *   Geometry
 */
private[apache] case class ST_LineMerge(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.lineMerge _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Azimuth(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.azimuth _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_X(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.x _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Y(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.y _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Z(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.z _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Zmflag(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.zmFlag _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_StartPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.startPoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Snap(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.snap _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Boundary(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.boundary _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MinimumClearance(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.minimumClearance _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MinimumClearanceLine(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.minimumClearanceLine _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MinimumBoundingRadius(inputExpressions: Seq[Expression])
    extends Expression
    with FoldableExpression
    with CodegenFallback {

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val expr = inputExpressions(0)
    val geometry = expr.toGeometry(input)

    try {
      geometry match {
        case geometry: Geometry => getMinimumBoundingRadius(geometry)
        case _ => null
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(geometry),
          e)
    }
  }

  private def getMinimumBoundingRadius(geom: Geometry): InternalRow = {
    val minimumBoundingCircle = new MinimumBoundingCircle(geom)
    val centerPoint = geom.getFactory.createPoint(minimumBoundingCircle.getCentre)
    InternalRow(centerPoint.toGenericArrayData, minimumBoundingCircle.getRadius)
  }

  override def dataType: DataType = DataTypes.createStructType(
    Array(
      DataTypes.createStructField("center", GeometryUDT(), false),
      DataTypes.createStructField("radius", DataTypes.DoubleType, false)))

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MinimumBoundingCircle(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.minimumBoundingCircle _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_OrientedEnvelope(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.orientedEnvelope _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_HasZ(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.hasZ _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_HasM(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.hasM _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_M(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.m _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MMin(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.mMin _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MMax(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.mMax _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_LineSegments(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.lineSegments),
      inferrableFunction1(Functions.lineSegments)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return a linestring being a substring of the input one starting and ending at the given
 * fractions of total 2d length. Second and third arguments are Double values between 0 and 1.
 * This only works with LINESTRINGs.
 *
 * @param inputExpressions
 */
private[apache] case class ST_LineSubstring(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.lineSubString _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a point interpolated along a line. First argument must be a LINESTRING. Second argument
 * is a Double between 0 and 1 representing fraction of total linestring length the point has to
 * be located.
 *
 * @param inputExpressions
 */
private[apache] case class ST_LineInterpolatePoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.lineInterpolatePoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a float between 0 and 1 representing the location of the closest point on a LineString
 * to the given Point, as a fraction of 2d line length.
 *
 * @param inputExpressions
 */
private[apache] case class ST_LineLocatePoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.lineLocatePoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_EndPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.endPoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ExteriorRing(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.exteriorRing _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_GeometryN(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.geometryN _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_InteriorRingN(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.interiorRingN _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Dump(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.dump _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_DumpPoints(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.dumpPoints _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_IsClosed(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.isClosed _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_NumInteriorRings(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.numInteriorRings _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_NumInteriorRing(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.numInteriorRings _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AddMeasure(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction3(Functions.addMeasure)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AddPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction3(Functions.addPoint)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_RemovePoint(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.removePoint)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_RemoveRepeatedPoints(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.removeRepeatedPoints),
      inferrableFunction1(Functions.removeRepeatedPoints)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SetPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.setPoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ClosestPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.closestPoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_IsPolygonCW(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.isPolygonCW _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_IsRing(inputExpressions: Seq[Expression])
    extends InferredExpression(ST_IsRing.isRing _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] object ST_IsRing {
  def isRing(geom: Geometry): Option[Boolean] = {
    geom match {
      case _: LineString => Some(Functions.isRing(geom))
      case _ => None
    }
  }
}

/**
 * Returns the number of Geometries. If geometry is a GEOMETRYCOLLECTION (or MULTI*) return the
 * number of geometries, for single geometries will return 1
 *
 * This method implements the SQL/MM specification. SQL-MM 3: 9.1.4
 *
 * @param inputExpressions
 *   Geometry
 */
private[apache] case class ST_NumGeometries(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.numGeometries _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a version of the given geometry with X and Y axis flipped.
 *
 * @param inputExpressions
 *   Geometry
 */
private[apache] case class ST_FlipCoordinates(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.flipCoordinates _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SubDivide(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.subDivide _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_SubDivideExplode(children: Seq[Expression])
    extends Generator
    with CodegenFallback {
  children.validateLength(2)

  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val geometry = children.head.toGeometry(input)
    val maxVertices = children(1).toInt(input)
    try {
      geometry match {
        case geom: Geometry =>
          ArrayData.toArrayData(Functions.subDivide(geom, maxVertices).map(_.toGenericArrayData))
          Functions
            .subDivide(geom, maxVertices)
            .map(_.toGenericArrayData)
            .map(InternalRow(_))
        case _ => new Array[InternalRow](0)
      }
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(geometry, maxVertices),
          e)
    }
  }

  override def elementSchema: StructType = {
    new StructType()
      .add("geom", GeometryUDT(), true)
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(children = newChildren)
  }
}

private[apache] case class ST_Segmentize(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.segmentize _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MakeLine(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.makeLine),
      inferrableFunction1(Functions.makeLine)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Perimeter(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.perimeter),
      inferrableFunction2(Functions.perimeter),
      inferrableFunction1(Functions.perimeter)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Perimeter2D(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.perimeter),
      inferrableFunction2(Functions.perimeter),
      inferrableFunction1(Functions.perimeter)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Points(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.points _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Polygon(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.makepolygonWithSRID _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Polygonize(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.polygonize _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Project(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction4(Functions.project),
      inferrableFunction3(Functions.project)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MakePolygon(inputExpressions: Seq[Expression])
    extends InferredExpression(InferrableFunction.allowRightNull(Functions.makePolygon)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_MaximumInscribedCircle(children: Seq[Expression])
    extends Expression
    with CodegenFallback {

  override def eval(input: InternalRow): Any = {
    val geometry = children.head.toGeometry(input)
    try {
      var inscribedCircle: InscribedCircle = null
      inscribedCircle = Functions.maximumInscribedCircle(geometry)

      val serCenter = GeometrySerializer.serialize(inscribedCircle.center)
      val serNearest = GeometrySerializer.serialize(inscribedCircle.nearest)
      InternalRow.fromSeq(Seq(serCenter, serNearest, inscribedCircle.radius))
    } catch {
      case e: Exception =>
        InferredExpression.throwExpressionInferenceException(
          getClass.getSimpleName,
          Seq(geometry),
          e)
    }
  }

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(children = newChildren)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = new StructType()
    .add("center", GeometryUDT(), nullable = false)
    .add("nearest", GeometryUDT(), nullable = false)
    .add("radius", DoubleType, nullable = false)
}

private[apache] case class ST_MaxDistance(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.maxDistance _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_GeoHash(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.geohash _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_GeoHashNeighbors(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.geohashNeighbors _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_GeoHashNeighbor(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.geohashNeighbor _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the difference between geometry A and B
 *
 * @param inputExpressions
 */
private[apache] case class ST_Difference(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.difference _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the symmetrical difference between geometry A and B
 *
 * @param inputExpressions
 */
private[apache] case class ST_SymDifference(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.symDifference _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_UnaryUnion(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction1(Functions.unaryUnion)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the union of geometry A and B
 *
 * @param inputExpressions
 */
private[apache] case class ST_Union(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.union),
      inferrableFunction1(Functions.union)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Multi(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.createMultiGeometryFromOneElement _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a POINT guaranteed to lie on the surface.
 *
 * @param inputExpressions
 *   Geometry
 */
private[apache] case class ST_PointOnSurface(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.pointOnSurface _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the geometry with vertex order reversed
 *
 * @param inputExpressions
 */
private[apache] case class ST_Reverse(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.reverse _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the nth point in the geometry, provided it is a linestring
 *
 * @param inputExpressions
 *   sequence of 2 input arguments, a geometry and a value 'n'
 */
private[apache] case class ST_PointN(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.pointN _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/*
 * Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates.
 *
 * @param inputExpressions
 */
private[apache] case class ST_Force_2D(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.force2D _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/*
 * Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates.
 *
 * @param inputExpressions
 */
private[apache] case class ST_Force2D(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.force2D _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the geometry in EWKT format
 *
 * @param inputExpressions
 */
private[apache] case class ST_AsEWKT(inputExpressions: Seq[Expression])
    extends InferredExpression(
      (geom: Geometry) => Functions.asEWKT(geom),
      (geog: Geography) => org.apache.sedona.common.geography.Functions.asEWKT(geog)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsGML(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.asGML _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AsKML(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.asKML _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if Geometry is empty geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_IsEmpty(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.isEmpty _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if returning Max X coordinate value.
 *
 * @param inputExpressions
 */
private[apache] case class ST_XMax(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.xMax _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Test if returning Min X coordinate value.
 *
 * @param inputExpressions
 */
private[apache] case class ST_XMin(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.xMin _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the areal geometry formed by the constituent linework of the input geometry assuming
 * all inner geometries represent holes
 *
 * @param inputExpressions
 */
private[apache] case class ST_BuildArea(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.buildArea _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the input geometry in its normalized form.
 *
 * @param inputExpressions
 */
private[apache] case class ST_Normalize(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.normalize _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns the LineString geometry given a MultiPoint geometry
 *
 * @param inputExpressions
 */
private[apache] case class ST_LineFromMultiPoint(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.lineFromMultiPoint _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a multi-geometry that is the result of splitting the input geometry by the blade
 * geometry
 *
 * @param inputExpressions
 */
private[apache] case class ST_Split(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.split _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_S2CellIDs(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.s2CellIDs _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_S2ToGeom(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.s2ToGeom _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_H3CellIDs(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.h3CellIDs _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_H3CellDistance(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.h3CellDistance _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_H3KRing(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.h3KRing _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_H3ToGeom(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.h3ToGeom _)
    with FoldableExpression {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_CollectionExtract(inputExpressions: Seq[Expression])
    extends InferredExpression(InferrableFunction.allowRightNull(Functions.collectionExtract)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a POINT Computes the approximate geometric median of a MultiPoint geometry using the
 * Weiszfeld algorithm. The geometric median provides a centrality measure that is less sensitive
 * to outlier points than the centroid.
 *
 * @param inputExpressions
 *   Geometry
 */
private[apache] case class ST_GeometricMedian(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction4(Functions.geometricMedian))
    with FoldableExpression {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_DistanceSphere(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Haversine.distance),
      inferrableFunction3(Haversine.distance)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_DistanceSpheroid(inputExpressions: Seq[Expression])
    extends InferredExpression(Spheroid.distance _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_AreaSpheroid(inputExpressions: Seq[Expression])
    extends InferredExpression(Spheroid.area _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_LengthSpheroid(inputExpressions: Seq[Expression])
    extends InferredExpression(Spheroid.length _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_LocateAlong(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.locateAlong),
      inferrableFunction2(Functions.locateAlong)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_LongestLine(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.longestLine _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_NumPoints(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.numPoints _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Force3D(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.force3D)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Force3DZ(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.force3D)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Force3DM(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.force3DM)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Force4D(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.force4D),
      inferrableFunction1(Functions.force4D)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ForceCollection(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.forceCollection _) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ForcePolygonCW(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.forcePolygonCW _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ForceRHR(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.forcePolygonCW _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_GeneratePoints(inputExpressions: Seq[Expression], randomSeed: Long)
    extends Expression
    with CodegenFallback
    with ExpectsInputTypes
    with Nondeterministic {

  def this(inputExpressions: Seq[Expression]) = this(inputExpressions, Utils.random.nextLong())

  @transient private[this] var random: java.util.Random = _

  private val nArgs = children.length

  override protected def initializeInternal(partitionIndex: Int): Unit = random =
    new java.util.Random(randomSeed + partitionIndex)

  override protected def evalInternal(input: InternalRow): Any = {
    val geom = children.head.toGeometry(input)
    val numPoints = children(1).eval(input).asInstanceOf[Int]
    val generatedPoints = if (nArgs == 3) {
      val seed = children(2).eval(input).asInstanceOf[Int]
      if (seed > 0) {
        Functions.generatePoints(geom, numPoints, seed)
      } else {
        Functions.generatePoints(geom, numPoints, random)
      }
    } else {
      Functions.generatePoints(geom, numPoints, random)
    }
    GeometrySerializer.serialize(generatedPoints)
  }

  override def nullable: Boolean = true

  override def dataType: DataType = GeometryUDT()

  override def inputTypes: Seq[AbstractDataType] = {
    if (nArgs == 3) {
      Seq(GeometryUDT(), IntegerType, IntegerType)
    } else if (nArgs == 2) {
      Seq(GeometryUDT(), IntegerType)
    } else {
      throw new IllegalArgumentException(s"Invalid number of arguments: $nArgs")
    }
  }

  override def children: Seq[Expression] = inputExpressions

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_NRings(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.nRings _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_IsPolygonCCW(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.isPolygonCCW _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_ForcePolygonCCW(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.forcePolygonCCW _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Translate(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction4(Functions.translate))
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_TriangulatePolygon(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.triangulatePolygon _)
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_VoronoiPolygons(inputExpressions: Seq[Expression])
    extends InferredExpression(nullTolerantInferrableFunction3(FunctionsGeoTools.voronoiPolygons))
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_FrechetDistance(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.frechetDistance _)
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Affine(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction13(Functions.affine),
      inferrableFunction7(Functions.affine)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Dimension(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.dimension _)
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_BoundingDiagonal(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.boundingDiagonal _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_HausdorffDistance(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.hausdorffDistance),
      inferrableFunction2(Functions.hausdorffDistance)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Angle(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction4(Functions.angle _),
      inferrableFunction3(Functions.angle _),
      inferrableFunction2(Functions.angle _))
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class GeometryType(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.geometryTypeWithMeasured _)
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_Degrees(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.degrees _)
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

private[apache] case class ST_DelaunayTriangles(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.delaunayTriangle),
      inferrableFunction2(Functions.delaunayTriangle),
      inferrableFunction1(Functions.delaunayTriangle))
    with FoldableExpression {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Return the number of dimensions in geometry.
 *
 * @param inputExpressions
 */
private[apache] case class ST_CoordDim(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.nDims _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns True if geometry is a collection of geometries
 *
 * @param inputExpressions
 */
private[apache] case class ST_IsCollection(inputExpressions: Seq[Expression])
    extends InferredExpression(Functions.isCollection _) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    copy(inputExpressions = newChildren)
  }
}

/**
 * Returns a text description of the validity of the geometry considering the specified flags. If
 * flag not specified, it defaults to OGC SFS validity semantics.
 *
 * @param geom
 *   The geometry to validate.
 * @param flag
 *   The validation flags.
 * @return
 *   A string describing the validity of the geometry.
 */
private[apache] case class ST_IsValidReason(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.isValidReason),
      inferrableFunction1(Functions.isValidReason)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ST_Scale(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction3(Functions.scale)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ST_ScaleGeom(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction3(Functions.scaleGeom),
      inferrableFunction2(Functions.scaleGeom)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ST_RotateX(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.rotateX)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ST_RotateY(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.rotateY)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ST_Rotate(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.rotate),
      inferrableFunction3(Functions.rotate),
      inferrableFunction4(Functions.rotate)) {

  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ST_InterpolatePoint(inputExpressions: Seq[Expression])
    extends InferredExpression(inferrableFunction2(Functions.interpolatePoint)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

/**
 * Computes the straight skeleton of an areal geometry. The straight skeleton is a method of
 * representing a polygon by a topological skeleton, formed by a continuous shrinking process
 * where each edge moves inward in parallel at a uniform speed.
 *
 * @param inputExpressions
 *   Geometry (Polygon or MultiPolygon), optional: maxVertices (Integer) for vertex limit
 */
private[apache] case class ST_StraightSkeleton(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.straightSkeleton),
      inferrableFunction1(Functions.straightSkeleton)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

/**
 * Computes an approximate medial axis of an areal geometry by computing the straight skeleton and
 * filtering to keep only interior edges. Edges where both endpoints are interior to the polygon
 * (not on the boundary) are kept, producing a cleaner skeleton.
 *
 * @param inputExpressions
 *   Geometry (Polygon or MultiPolygon), optional: maxVertices (Integer) for vertex limit
 */
private[apache] case class ST_ApproximateMedialAxis(inputExpressions: Seq[Expression])
    extends InferredExpression(
      inferrableFunction2(Functions.approximateMedialAxis),
      inferrableFunction1(Functions.approximateMedialAxis)) {
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]) =
    copy(inputExpressions = newChildren)
}

private[apache] case class ExpandAddress(address: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with FoldableExpression
    with Serializable {

  def this(children: Seq[Expression]) = this(children.head)

  lazy private final val expander = {
    val conf = SedonaConf.fromSparkEnv
    getExpanderFromConf(conf.getLibPostalDataDir, conf.getLibPostalUseSenzing)
  }

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = ArrayType(StringType)

  override def eval(input: InternalRow): Any = {
    val addressVal = address.eval(input)
    if (addressVal == null) {
      null
    } else {
      new GenericArrayData(
        expander
          .expandAddress(addressVal.asInstanceOf[UTF8String].toString)
          .map(UTF8String.fromString))
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expanderRef = ctx.addReferenceObj("expander", expander, classOf[AddressExpander].getName)
    val utf8StringClass = "org.apache.spark.unsafe.types.UTF8String"
    val arrayDataClass = "org.apache.spark.sql.catalyst.util.GenericArrayData"
    val addressRef = child.genCode(ctx)
    val address = addressRef.value
    ev.copy(code = code"""
          ${addressRef.code}
          boolean ${ev.isNull} = ${addressRef.isNull};
          $arrayDataClass ${ev.value} = null;

          if (!${ev.isNull}) {
            String[] expandedAddressArray = $expanderRef.expandAddress($address.toString());
            $utf8StringClass[] utf8Strings = new $utf8StringClass[expandedAddressArray.length];
            for (int j = 0; j < expandedAddressArray.length; j++) {
              utf8Strings[j] = $utf8StringClass.fromString(expandedAddressArray[j]);
            }
            ${ev.value} = new $arrayDataClass(utf8Strings);
          } else {
            ${ev.value} = null;
          }""".stripMargin)
  }

  override def toString: String = s"ExpandAddress($address)"

  override def child: Expression = address

  override protected def withNewChildInternal(newChild: Expression): Expression =
    ExpandAddress(newChild)
}

private[apache] case class ParseAddress(address: Expression)
    extends UnaryExpression
    with ImplicitCastInputTypes
    with CodegenFallback
    with FoldableExpression
    with Serializable {

  def this(children: Seq[Expression]) = this(children.head)

  lazy private final val parser = {
    val conf = SedonaConf.fromSparkEnv
    getParserFromConf(conf.getLibPostalDataDir, conf.getLibPostalUseSenzing)
  }

  override def nullable: Boolean = true

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = ArrayType(
    StructType(Seq(StructField("label", StringType), StructField("value", StringType))))

  override def eval(input: InternalRow): Any = {
    val addressVal = address.eval(input)
    if (addressVal == null) {
      null
    } else {
      new GenericArrayData(
        parser
          .parseAddress(addressVal.asInstanceOf[UTF8String].toString)
          .map(component =>
            InternalRow(
              UTF8String
                .fromString(component.getLabel),
              UTF8String.fromString(component.getValue))))
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val parserRef = ctx.addReferenceObj("parser", parser, classOf[AddressParser].getName)
    val arrayDataClass = "org.apache.spark.sql.catalyst.util.GenericArrayData"
    val internalRowClass = "org.apache.spark.sql.catalyst.expressions.GenericInternalRow"

    val addressRef = child.genCode(ctx)
    val address = addressRef.value
    val code = code"""
        ${addressRef.code}
        boolean ${ev.isNull} = ${addressRef.isNull};
        $arrayDataClass ${ev.value};

        if (!${ev.isNull}) {
          com.mapzen.jpostal.ParsedComponent[] components = $parserRef.parseAddress($address.toString());
          $internalRowClass[] rows = new $internalRowClass[components.length];
          for (int j = 0; j < components.length; j++) {
            Object[] fields = new Object[2];
            fields[0] = UTF8String.fromString(components[j].getLabel());
            fields[1] = UTF8String.fromString(components[j].getValue());
            $internalRowClass row = new GenericInternalRow(fields);
            rows[j] = row;
          }
          ${ev.value} = new $arrayDataClass(rows);
        } else {
          ${ev.value} = null;
        }""".stripMargin
    ev.copy(code = code)
  }

  override def toString: String = s"ParseAddress($address)"

  override def child: Expression = address

  override protected def withNewChildInternal(newChild: Expression): Expression = ParseAddress(
    newChild)
}
