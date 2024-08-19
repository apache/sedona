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
package org.apache.sedona.sql.UDF

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.sedona_sql.expressions._
import org.apache.spark.sql.sedona_sql.expressions.collect.ST_Collect
import org.apache.spark.sql.sedona_sql.expressions.raster._
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.buffer.BufferParameters

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object Catalog {

  type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)

  val expressions: Seq[FunctionDescription] = Seq(
    // Expression for vectors
    function[GeometryType](),
    function[ST_PointFromText](),
    function[ST_PointFromWKB](),
    function[ST_LineFromWKB](),
    function[ST_LinestringFromWKB](),
    function[ST_PolygonFromText](),
    function[ST_LineStringFromText](),
    function[ST_GeomFromText](0),
    function[ST_GeometryFromText](0),
    function[ST_LineFromText](),
    function[ST_GeomFromWKT](0),
    function[ST_GeomFromEWKT](),
    function[ST_GeomFromWKB](),
    function[ST_GeomFromEWKB](),
    function[ST_GeomFromGeoJSON](),
    function[ST_GeomFromGML](),
    function[ST_GeomFromKML](),
    function[ST_CoordDim](),
    function[ST_Point](),
    function[ST_Points](),
    function[ST_MakePoint](null, null),
    function[ST_MakePointM](),
    function[ST_PointZ](0),
    function[ST_PointM](0),
    function[ST_PointZM](0),
    function[ST_PolygonFromEnvelope](),
    function[ST_Contains](),
    function[ST_Intersects](),
    function[ST_Within](),
    function[ST_Covers](),
    function[ST_CoveredBy](),
    function[ST_Dimension](),
    function[ST_Disjoint](),
    function[ST_Distance](),
    function[ST_3DDistance](),
    function[ST_ConcaveHull](false),
    function[ST_ConvexHull](),
    function[ST_NPoints](),
    function[ST_NDims](),
    function[ST_Buffer](),
    function[ST_BestSRID](),
    function[ST_ShiftLongitude](),
    function[ST_Envelope](),
    function[ST_Expand](),
    function[ST_Length](),
    function[ST_Length2D](),
    function[ST_Area](),
    function[ST_Centroid](),
    function[ST_Transform](true),
    function[ST_Intersection](),
    function[ST_Difference](),
    function[ST_SymDifference](),
    function[ST_UnaryUnion](),
    function[ST_Union](),
    function[ST_IsValidDetail](),
    function[ST_IsValidTrajectory](),
    function[ST_IsValid](),
    function[ST_IsEmpty](),
    function[ST_ReducePrecision](),
    function[ST_Equals](),
    function[ST_Touches](),
    function[ST_Relate](),
    function[ST_RelateMatch](),
    function[ST_Overlaps](),
    function[ST_Crosses](),
    function[ST_CrossesDateLine](),
    function[ST_IsSimple](),
    function[ST_MakeValid](false),
    function[ST_SimplifyPreserveTopology](),
    function[ST_AsText](),
    function[ST_AsGeoJSON](),
    function[ST_AsBinary](),
    function[ST_AsEWKB](),
    function[ST_AsHEXEWKB](),
    function[ST_AsGML](),
    function[ST_AsKML](),
    function[ST_SimplifyVW](),
    function[ST_SimplifyPolygonHull](),
    function[ST_SRID](),
    function[ST_SetSRID](),
    function[ST_GeometryType](),
    function[ST_NumGeometries](),
    function[ST_LineMerge](),
    function[ST_Azimuth](),
    function[ST_X](),
    function[ST_Y](),
    function[ST_Z](),
    function[ST_Zmflag](),
    function[ST_StartPoint](),
    function[ST_Snap](),
    function[ST_ClosestPoint](),
    function[ST_Boundary](),
    function[ST_HasZ](),
    function[ST_HasM](),
    function[ST_M](),
    function[ST_MMin](),
    function[ST_MMax](),
    function[ST_MinimumClearance](),
    function[ST_MinimumClearanceLine](),
    function[ST_MinimumBoundingRadius](),
    function[ST_MinimumBoundingCircle](BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6),
    function[ST_EndPoint](),
    function[ST_ExteriorRing](),
    function[ST_GeometryN](),
    function[ST_H3CellDistance](),
    function[ST_H3CellIDs](),
    function[ST_H3ToGeom](),
    function[ST_H3KRing](),
    function[ST_InteriorRingN](),
    function[ST_Dump](),
    function[ST_DumpPoints](),
    function[ST_IsClosed](),
    function[ST_IsCollection](),
    function[ST_NumInteriorRings](),
    function[ST_NumInteriorRing](),
    function[ST_AddMeasure](),
    function[ST_AddPoint](-1),
    function[ST_RemovePoint](-1),
    function[ST_SetPoint](),
    function[ST_IsPolygonCW](),
    function[ST_IsRing](),
    function[ST_IsPolygonCCW](),
    function[ST_ForcePolygonCCW](),
    function[ST_FlipCoordinates](),
    function[ST_LineSubstring](),
    function[ST_LineInterpolatePoint](),
    function[ST_LineLocatePoint](),
    function[ST_LocateAlong](),
    function[ST_LongestLine](),
    function[ST_SubDivideExplode](),
    function[ST_SubDivide](),
    function[ST_MakeLine](),
    function[ST_Polygon](),
    function[ST_Polygonize](),
    function[ST_MakePolygon](null),
    function[ST_MaximumInscribedCircle](),
    function[ST_MaxDistance](),
    function[ST_GeoHash](),
    function[ST_GeomFromGeoHash](null),
    function[ST_PointFromGeoHash](null),
    function[ST_Collect](),
    function[ST_Multi](),
    function[ST_PointOnSurface](),
    function[ST_Reverse](),
    function[ST_PointN](),
    function[ST_AsEWKT](),
    function[ST_Force_2D](),
    function[ST_ForcePolygonCW](),
    function[ST_ForceRHR](),
    function[ST_ZMax](),
    function[ST_ZMin](),
    function[ST_YMax](),
    function[ST_YMin](),
    function[ST_XMax](),
    function[ST_XMin](),
    function[ST_BuildArea](),
    function[ST_OrderingEquals](),
    function[ST_CollectionExtract](defaultArgs = null),
    function[ST_Normalize](),
    function[ST_LineFromMultiPoint](),
    function[ST_MPointFromText](0),
    function[ST_MPolyFromText](0),
    function[ST_MLineFromText](0),
    function[ST_GeomCollFromText](0),
    function[ST_Split](),
    function[ST_S2CellIDs](),
    function[ST_S2ToGeom](),
    function[ST_GeometricMedian](1e-6, 1000, false),
    function[ST_DistanceSphere](),
    function[ST_DistanceSpheroid](),
    function[ST_AreaSpheroid](),
    function[ST_LengthSpheroid](),
    function[ST_NumPoints](),
    function[ST_Force3D](0.0),
    function[ST_Force3DM](0.0),
    function[ST_Force3DZ](0.0),
    function[ST_Force4D](),
    function[ST_ForceCollection](),
    function[ST_GeneratePoints](),
    function[ST_NRings](),
    function[ST_Translate](0.0),
    function[ST_TriangulatePolygon](),
    function[ST_VoronoiPolygons](0.0, null),
    function[ST_FrechetDistance](),
    function[ST_Affine](),
    function[ST_BoundingDiagonal](),
    function[ST_Angle](),
    function[ST_Degrees](),
    function[ST_DelaunayTriangles](),
    function[ST_HausdorffDistance](-1),
    function[ST_DWithin](),
    function[ST_IsValidReason](),
    function[ST_Rotate](),
    function[ST_RotateX](),
    // Expression for rasters
    function[RS_NormalizedDifference](),
    function[RS_Mean](),
    function[RS_Mode](),
    function[RS_FetchRegion](),
    function[RS_GreaterThan](),
    function[RS_GreaterThanEqual](),
    function[RS_LessThan](),
    function[RS_LessThanEqual](),
    function[RS_Add](),
    function[RS_Subtract](),
    function[RS_Divide](),
    function[RS_MultiplyFactor](),
    function[RS_Multiply](),
    function[RS_BitwiseAnd](),
    function[RS_BitwiseOr](),
    function[RS_CountValue](),
    function[RS_Modulo](),
    function[RS_SquareRoot](),
    function[RS_LogicalDifference](),
    function[RS_LogicalOver](),
    function[RS_Array](),
    function[RS_Normalize](),
    function[RS_NormalizeAll](),
    function[RS_AddBandFromArray](),
    function[RS_BandAsArray](),
    function[RS_MapAlgebra](null),
    function[RS_FromArcInfoAsciiGrid](),
    function[RS_FromGeoTiff](),
    function[RS_MakeEmptyRaster](),
    function[RS_MakeRaster](),
    function[RS_MakeRasterForTesting](),
    function[RS_Tile](),
    function[RS_TileExplode](),
    function[RS_Envelope](),
    function[RS_NumBands](),
    function[RS_Metadata](),
    function[RS_SetSRID](),
    function[RS_SetGeoReference](),
    function[RS_SetBandNoDataValue](),
    function[RS_SetPixelType](),
    function[RS_SetValues](),
    function[RS_SetValue](),
    function[RS_SRID](),
    function[RS_Value](1),
    function[RS_Values](1),
    function[RS_Intersects](),
    function[RS_Interpolate](),
    function[RS_AsGeoTiff](),
    function[RS_AsRaster](),
    function[RS_AsArcGrid](),
    function[RS_AsBase64](),
    function[RS_AsPNG](),
    function[RS_Width](),
    function[RS_Height](),
    function[RS_Union](),
    function[RS_UpperLeftX](),
    function[RS_UpperLeftY](),
    function[RS_ScaleX](),
    function[RS_ScaleY](),
    function[RS_SkewX](),
    function[RS_SkewY](),
    function[RS_GeoReference](),
    function[RS_Rotation](),
    function[RS_GeoTransform](),
    function[RS_PixelAsPoint](),
    function[RS_PixelAsPoints](),
    function[RS_PixelAsPolygon](),
    function[RS_PixelAsPolygons](),
    function[RS_PixelAsCentroid](),
    function[RS_PixelAsCentroids](),
    function[RS_Count](),
    function[RS_Clip](),
    function[RS_Band](),
    function[RS_AddBand](),
    function[RS_SummaryStatsAll](),
    function[RS_SummaryStats](),
    function[RS_BandIsNoData](),
    function[RS_ConvexHull](),
    function[RS_RasterToWorldCoordX](),
    function[RS_RasterToWorldCoordY](),
    function[RS_RasterToWorldCoord](),
    function[RS_Within](),
    function[RS_Contains](),
    function[RS_WorldToRasterCoord](),
    function[RS_WorldToRasterCoordX](),
    function[RS_WorldToRasterCoordY](),
    function[RS_BandNoDataValue](),
    function[RS_BandPixelType](),
    function[RS_MinConvexHull](),
    function[RS_AsMatrix](),
    function[RS_AsImage](),
    function[RS_ZonalStats](),
    function[RS_ZonalStatsAll](),
    function[RS_Resample](),
    function[RS_ReprojectMatch]("nearestneighbor"),
    function[RS_FromNetCDF](),
    function[RS_NetCDFInfo]())

  // Aggregate functions with Geometry as buffer
  val aggregateExpressions: Seq[Aggregator[Geometry, Geometry, Geometry]] =
    Seq(new ST_Envelope_Aggr, new ST_Intersection_Aggr)

  // Aggregate functions with List as buffer
  val aggregateExpressions2: Seq[Aggregator[Geometry, ListBuffer[Geometry], Geometry]] =
    Seq(new ST_Union_Aggr())

  private def function[T <: Expression: ClassTag](defaultArgs: Any*): FunctionDescription = {
    val classTag = implicitly[ClassTag[T]]
    val constructor = classTag.runtimeClass.getConstructor(classOf[Seq[Expression]])
    val functionName = classTag.runtimeClass.getSimpleName
    val functionIdentifier = FunctionIdentifier(functionName)
    val expressionInfo = new ExpressionInfo(
      classTag.runtimeClass.getCanonicalName,
      functionIdentifier.database.orNull,
      functionName)

    def functionBuilder(expressions: Seq[Expression]): T = {
      val expr = constructor.newInstance(expressions).asInstanceOf[T]
      expr match {
        case e: ExpectsInputTypes =>
          val numParameters = e.inputTypes.size
          val numArguments = expressions.size
          if (numParameters == numArguments) expr
          else {
            val numUnspecifiedArgs = numParameters - numArguments
            if (numUnspecifiedArgs > 0) {
              if (numUnspecifiedArgs <= defaultArgs.size) {
                val args =
                  expressions ++ defaultArgs.takeRight(numUnspecifiedArgs).map(Literal(_))
                constructor.newInstance(args).asInstanceOf[T]
              } else {
                throw new IllegalArgumentException(s"function $functionName takes at least " +
                  s"${numParameters - defaultArgs.size} argument(s), $numArguments argument(s) specified")
              }
            } else {
              throw new IllegalArgumentException(
                s"function $functionName takes at most " +
                  s"$numParameters argument(s), $numArguments argument(s) specified")
            }
          }
        case _ => expr
      }
    }

    (functionIdentifier, expressionInfo, functionBuilder)
  }
}
