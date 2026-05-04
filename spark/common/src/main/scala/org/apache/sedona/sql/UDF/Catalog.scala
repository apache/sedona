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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.sedona_sql.expressions.collect.ST_Collect
import org.apache.spark.sql.sedona_sql.expressions.raster._
import org.apache.spark.sql.sedona_sql.expressions._
import org.apache.spark.sql.sedona_sql.expressions.geography.{ST_GeogCollFromText, ST_GeogFromEWKB, ST_GeogFromEWKT, ST_GeogFromGeoHash, ST_GeogFromText, ST_GeogFromWKB, ST_GeogFromWKT, ST_GeogToGeometry, ST_GeomToGeography}
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.operation.buffer.BufferParameters

/**
 * Sedona SQL function registry.
 *
 * Functions are organized into named sequences whose names mirror the categories used by the
 * public docs at https://sedona.apache.org/latest/api/sql/Geometry-Functions/ and the
 * corresponding raster docs pages, so the code-level taxonomy stays in lockstep with the docs
 * taxonomy. See `CatalogCategorizationTest` for an invariant that every registered function
 * belongs to exactly one named sequence.
 */
object Catalog extends AbstractCatalog with Logging {

  // ===========================================================================
  // Geometry (ST_) functions — categories from /api/sql/Geometry-Functions/
  // ===========================================================================

  // Geometry-Constructors
  val geometryConstructorExprs: Seq[FunctionDescription] = Seq(
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
    function[ST_Point](),
    function[ST_MakeBox2D](),
    function[ST_MakeEnvelope](),
    function[ST_MakePoint](null, null),
    function[ST_MakePointM](),
    function[ST_PointZ](0),
    function[ST_PointM](0),
    function[ST_PointZM](0),
    function[ST_PolygonFromEnvelope](),
    function[ST_GeomFromGeoHash](null),
    function[ST_PointFromGeoHash](null),
    function[ST_GeomFromMySQL](),
    function[ST_MPointFromText](0),
    function[ST_MPolyFromText](0),
    function[ST_MLineFromText](0),
    function[ST_GeomCollFromText](0))

  // Geometry-Accessors
  val geometryAccessorExprs: Seq[FunctionDescription] = Seq(
    function[GeometryType](),
    function[ST_Boundary](),
    function[ST_CoordDim](),
    function[ST_CrossesDateLine](),
    function[ST_Dimension](),
    function[ST_Dump](),
    function[ST_DumpPoints](),
    function[ST_EndPoint](),
    function[ST_ExteriorRing](),
    function[ST_GeometryN](),
    function[ST_GeometryType](),
    function[ST_HasM](),
    function[ST_HasZ](),
    function[ST_InteriorRingN](),
    function[ST_IsClosed](),
    function[ST_IsCollection](),
    function[ST_IsEmpty](),
    function[ST_IsPolygonCCW](),
    function[ST_IsPolygonCW](),
    function[ST_IsRing](),
    function[ST_IsSimple](),
    function[ST_M](),
    function[ST_NDims](),
    function[ST_NPoints](),
    function[ST_NRings](),
    function[ST_NumGeometries](),
    function[ST_NumInteriorRing](),
    function[ST_NumInteriorRings](),
    function[ST_NumPoints](),
    function[ST_PointN](),
    function[ST_Points](),
    function[ST_StartPoint](),
    function[ST_X](),
    function[ST_Y](),
    function[ST_Z](),
    function[ST_Zmflag]())

  // Geometry-Editors
  val geometryEditorExprs: Seq[FunctionDescription] = Seq(
    function[ST_AddPoint](-1),
    function[ST_Collect](),
    function[ST_CollectionExtract](defaultArgs = null),
    function[ST_FlipCoordinates](),
    function[ST_Force2D](),
    function[ST_Force3D](0.0),
    function[ST_Force3DM](0.0),
    function[ST_Force3DZ](0.0),
    function[ST_Force4D](),
    function[ST_Force_2D](),
    function[ST_ForceCollection](),
    function[ST_ForcePolygonCCW](),
    function[ST_ForcePolygonCW](),
    function[ST_ForceRHR](),
    function[ST_LineFromMultiPoint](),
    function[ST_LineMerge](),
    function[ST_LineSegments](),
    function[ST_MakeLine](),
    function[ST_MakePolygon](null),
    function[ST_Multi](),
    function[ST_Normalize](),
    function[ST_Polygon](),
    function[ST_Project](),
    function[ST_RemovePoint](-1),
    function[ST_RemoveRepeatedPoints](),
    function[ST_Reverse](),
    function[ST_Segmentize](),
    function[ST_SetPoint](),
    function[ST_ShiftLongitude]())

  // Geometry-Output
  val geometryOutputExprs: Seq[FunctionDescription] = Seq(
    function[ST_AsBinary](),
    function[ST_AsEWKB](),
    function[ST_AsEWKT](),
    function[ST_AsGeoJSON](),
    function[ST_AsGML](),
    function[ST_AsHEXEWKB](),
    function[ST_AsKML](),
    function[ST_AsText](),
    function[ST_GeoHash]())

  // Predicates
  val predicateExprs: Seq[FunctionDescription] = Seq(
    function[ST_Contains](),
    function[ST_CoveredBy](),
    function[ST_Covers](),
    function[ST_Crosses](),
    function[ST_Disjoint](),
    function[ST_DWithin](),
    function[ST_Equals](),
    function[ST_Intersects](),
    function[ST_OrderingEquals](),
    function[ST_Overlaps](),
    function[ST_Relate](),
    function[ST_RelateMatch](),
    function[ST_Touches](),
    function[ST_Within]())

  // Measurement-Functions
  val measurementExprs: Seq[FunctionDescription] = Seq(
    function[ST_3DDistance](),
    function[ST_Angle](),
    function[ST_Area](),
    function[ST_AreaSpheroid](),
    function[ST_Azimuth](),
    function[ST_ClosestPoint](),
    function[ST_Degrees](),
    function[ST_Distance](),
    function[ST_DistanceSphere](),
    function[ST_DistanceSpheroid](),
    function[ST_FrechetDistance](),
    function[ST_HausdorffDistance](-1),
    function[ST_Length](),
    function[ST_Length2D](),
    function[ST_LengthSpheroid](),
    function[ST_LongestLine](),
    function[ST_MaxDistance](),
    function[ST_MinimumClearance](),
    function[ST_MinimumClearanceLine](),
    function[ST_Perimeter](),
    function[ST_Perimeter2D](),
    function[ST_ShortestLine]())

  // Geometry-Processing
  val geometryProcessingExprs: Seq[FunctionDescription] = Seq(
    function[ST_ApproximateMedialAxis](),
    function[ST_Buffer](),
    function[ST_BuildArea](),
    function[ST_Centroid](),
    function[ST_ConcaveHull](false),
    function[ST_ConvexHull](),
    function[ST_DelaunayTriangles](),
    function[ST_GeneratePoints](),
    function[ST_GeometricMedian](1e-6, 1000, false),
    function[ST_LabelPoint](),
    function[ST_MaximumInscribedCircle](),
    function[ST_MinimumBoundingCircle](BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6),
    function[ST_MinimumBoundingRadius](),
    function[ST_OffsetCurve](),
    function[ST_OrientedEnvelope](),
    function[ST_PointOnSurface](),
    function[ST_Polygonize](),
    function[ST_ReducePrecision](),
    function[ST_Simplify](),
    function[ST_SimplifyPolygonHull](),
    function[ST_SimplifyPreserveTopology](),
    function[ST_SimplifyVW](),
    function[ST_Snap](),
    function[ST_StraightSkeleton](),
    function[ST_TriangulatePolygon](),
    function[ST_VoronoiPolygons](0.0, null))

  // Overlay-Functions
  val overlayExprs: Seq[FunctionDescription] = Seq(
    function[ST_Difference](),
    function[ST_Intersection](),
    function[ST_Split](),
    function[ST_SubDivide](),
    function[ST_SubDivideExplode](),
    function[ST_SymDifference](),
    function[ST_UnaryUnion](),
    function[ST_Union]())

  // Affine-Transformations
  val affineTransformationExprs: Seq[FunctionDescription] = Seq(
    function[ST_Affine](),
    function[ST_Rotate](),
    function[ST_RotateX](),
    function[ST_RotateY](),
    function[ST_Scale](),
    function[ST_ScaleGeom](),
    function[ST_Translate](0.0))

  // Linear-Referencing
  val linearReferencingExprs: Seq[FunctionDescription] = Seq(
    function[ST_AddMeasure](),
    function[ST_InterpolatePoint](),
    function[ST_IsValidTrajectory](),
    function[ST_LineInterpolatePoint](),
    function[ST_LineLocatePoint](),
    function[ST_LineSubstring](),
    function[ST_LocateAlong]())

  // Spatial-Reference-System
  val spatialReferenceSystemExprs: Seq[FunctionDescription] = Seq(
    function[ST_BestSRID](),
    function[ST_SetSRID](),
    function[ST_SRID](),
    function[ST_Transform](true))

  // Geometry-Validation
  val geometryValidationExprs: Seq[FunctionDescription] = Seq(
    function[ST_IsValid](),
    function[ST_IsValidDetail](),
    function[ST_IsValidReason](),
    function[ST_MakeValid](false))

  // Bounding-Box-Functions
  val boundingBoxExprs: Seq[FunctionDescription] = Seq(
    function[ST_BoundingDiagonal](),
    function[ST_Box2D](),
    function[ST_Envelope](),
    function[ST_Expand](),
    function[ST_MMax](),
    function[ST_MMin](),
    function[ST_XMax](),
    function[ST_XMin](),
    function[ST_YMax](),
    function[ST_YMin](),
    function[ST_ZMax](),
    function[ST_ZMin]())

  // Spatial-Indexing — also receives ST_KNN, which has its own NearestNeighbourSearching.md
  // page but isn't listed under any of the 18 docs categories.
  val spatialIndexingExprs: Seq[FunctionDescription] = Seq(
    function[ST_BingTile](),
    function[ST_BingTileAt](),
    function[ST_BingTileCellIDs](),
    function[ST_BingTilePolygon](),
    function[ST_BingTilesAround](),
    function[ST_BingTileToGeom](),
    function[ST_BingTileX](),
    function[ST_BingTileY](),
    function[ST_BingTileZoomLevel](),
    function[ST_GeoHashNeighbor](),
    function[ST_GeoHashNeighbors](),
    function[ST_H3CellDistance](),
    function[ST_H3CellIDs](),
    function[ST_H3KRing](),
    function[ST_H3ToGeom](),
    function[ST_S2CellIDs](),
    function[ST_S2ToGeom](),
    function[ST_KNN]())

  // Address-Functions
  val addressExprs: Seq[FunctionDescription] =
    Seq(function[ExpandAddress](), function[ParseAddress]())

  // Other / utility expressions not in any docs category
  val otherExprs: Seq[FunctionDescription] = Seq(function[Barrier]())

  // Geography (ST_Geog*) — see docs/api/sql/geography/Geography-Functions
  val geographyExprs: Seq[FunctionDescription] = Seq(
    function[ST_GeogFromWKT](0),
    function[ST_GeogFromText](0),
    function[ST_GeogFromWKB](0),
    function[ST_GeogFromEWKB](0),
    function[ST_GeogFromEWKT](),
    function[ST_GeogCollFromText](0),
    function[ST_GeogFromGeoHash](null),
    function[ST_GeogToGeometry](),
    function[ST_GeomToGeography]())

  // ===========================================================================
  // Raster (RS_) functions — categories from the raster docs pages
  // ===========================================================================

  // Raster-Constructors
  val rasterConstructorExprs: Seq[FunctionDescription] = Seq(
    function[RS_FromArcInfoAsciiGrid](),
    function[RS_FromGeoTiff](),
    function[RS_FromNetCDF](),
    function[RS_MakeEmptyRaster](),
    function[RS_MakeRaster](),
    function[RS_MakeRasterForTesting](),
    function[RS_NetCDFInfo]())

  // Raster-Accessors (spatial properties: dimensions, scale, skew, world coords)
  val rasterAccessorExprs: Seq[FunctionDescription] = Seq(
    function[RS_GeoReference](),
    function[RS_GeoTransform](),
    function[RS_Height](),
    function[RS_RasterToWorldCoord](),
    function[RS_RasterToWorldCoordX](),
    function[RS_RasterToWorldCoordY](),
    function[RS_Rotation](),
    function[RS_ScaleX](),
    function[RS_ScaleY](),
    function[RS_SkewX](),
    function[RS_SkewY](),
    function[RS_UpperLeftX](),
    function[RS_UpperLeftY](),
    function[RS_Width](),
    function[RS_WorldToRasterCoord](),
    function[RS_WorldToRasterCoordX](),
    function[RS_WorldToRasterCoordY]())

  // Raster-Band-Accessors (band-level properties and statistics)
  val rasterBandAccessorExprs: Seq[FunctionDescription] = Seq(
    function[RS_Band](),
    function[RS_BandIsNoData](),
    function[RS_BandNoDataValue](),
    function[RS_BandPixelType](),
    function[RS_Count](),
    function[RS_SummaryStats](),
    function[RS_SummaryStatsAll](),
    function[RS_ZonalStats](),
    function[RS_ZonalStatsAll]())

  // Raster-Operators (configuration, transformation, manipulation)
  val rasterOperatorExprs: Seq[FunctionDescription] = Seq(
    function[RS_AddBand](),
    function[RS_AsRaster](),
    function[RS_Clip](),
    function[RS_CRS](),
    function[RS_Interpolate](),
    function[RS_Metadata](),
    function[RS_NormalizeAll](),
    function[RS_NumBands](),
    function[RS_ReprojectMatch]("nearestneighbor"),
    function[RS_Resample](),
    function[RS_SetBandNoDataValue](),
    function[RS_SetCRS](),
    function[RS_SetGeoReference](),
    function[RS_SetPixelType](),
    function[RS_SetSRID](),
    function[RS_SetValue](),
    function[RS_SetValues](),
    function[RS_SRID](),
    function[RS_Union](),
    function[RS_Value](1),
    function[RS_Values](1))

  // Raster-Output
  val rasterOutputExprs: Seq[FunctionDescription] = Seq(
    function[RS_AsArcGrid](),
    function[RS_AsBase64](),
    function[RS_AsCOG](),
    function[RS_AsGeoTiff](),
    function[RS_AsImage](),
    function[RS_AsMatrix](),
    function[RS_AsPNG]())

  // Raster-Predicates
  val rasterPredicateExprs: Seq[FunctionDescription] =
    Seq(function[RS_Contains](), function[RS_Intersects](), function[RS_Within]())

  // Raster-Geometry-Functions (raster → geometry derivations)
  val rasterGeometryExprs: Seq[FunctionDescription] =
    Seq(function[RS_ConvexHull](), function[RS_Envelope](), function[RS_MinConvexHull]())

  // Pixel-Functions
  val pixelExprs: Seq[FunctionDescription] = Seq(
    function[RS_PixelAsCentroid](),
    function[RS_PixelAsCentroids](),
    function[RS_PixelAsPoint](),
    function[RS_PixelAsPoints](),
    function[RS_PixelAsPolygon](),
    function[RS_PixelAsPolygons]())

  // Map-Algebra-Operators (per-pixel/per-band math operators)
  val mapAlgebraExprs: Seq[FunctionDescription] = Seq(
    function[RS_Add](),
    function[RS_Array](),
    function[RS_BitwiseAnd](),
    function[RS_BitwiseOr](),
    function[RS_CountValue](),
    function[RS_Divide](),
    function[RS_FetchRegion](),
    function[RS_GreaterThan](),
    function[RS_GreaterThanEqual](),
    function[RS_LessThan](),
    function[RS_LessThanEqual](),
    function[RS_LogicalDifference](),
    function[RS_LogicalOver](),
    function[RS_Mean](),
    function[RS_Mode](),
    function[RS_Modulo](),
    function[RS_Multiply](),
    function[RS_MultiplyFactor](),
    function[RS_Normalize](),
    function[RS_NormalizedDifference](),
    function[RS_SquareRoot](),
    function[RS_Subtract]())

  // Raster-Map-Algebra-Operators (the meta map algebra entry point and
  // band/array conversion helpers used to express map algebra)
  val rasterMapAlgebraExprs: Seq[FunctionDescription] = Seq(
    function[RS_AddBandFromArray](),
    function[RS_BandAsArray](),
    function[RS_MapAlgebra](null))

  // Raster-Tiles
  val rasterTileExprs: Seq[FunctionDescription] =
    Seq(function[RS_Tile](), function[RS_TileExplode]())

  // ===========================================================================
  // dbx-incompatible functions, split by docs category. May fail to load on
  // unsupported DBR versions — see https://github.com/apache/sedona/issues/2472.
  // ===========================================================================
  private val dbxIncompatibleGroups: (Seq[FunctionDescription], Seq[FunctionDescription]) = {
    try {
      // Clustering-Functions docs page
      val clustering = Seq(function[ST_DBSCAN](), function[ST_LocalOutlierFactor]())
      // Spatial-Statistics docs page
      val spatialStatistics = Seq(
        function[ST_GLocal](),
        function[ST_BinaryDistanceBandColumn](),
        function[ST_WeightedDistanceBandColumn]())
      (clustering, spatialStatistics)
    } catch {
      case e: Throwable =>
        log.warn(
          "clustering and spatial-statistics functions are not available due to Spark/DBR compatibility issues.",
          e)
        (Seq.empty, Seq.empty)
    }
  }
  val clusteringExprs: Seq[FunctionDescription] = dbxIncompatibleGroups._1
  val spatialStatisticsExprs: Seq[FunctionDescription] = dbxIncompatibleGroups._2

  // ===========================================================================
  // All named sequences. `expressions` is defined as the flatten of these.
  //
  // Note: this changes the registration order relative to the previous flat
  // `expressions` list (which had the docs categories interleaved). Order
  // doesn't affect registration semantics — `AbstractCatalog.registerAll`
  // registers each function exactly once regardless of order, and there are
  // no duplicate function identifiers across categories. The duplicate-name
  // invariant across sequences is checked by `CatalogCategorizationTest`.
  // ===========================================================================
  val categorizedSequences: Seq[Seq[FunctionDescription]] = Seq(
    geometryConstructorExprs,
    geometryAccessorExprs,
    geometryEditorExprs,
    geometryOutputExprs,
    predicateExprs,
    measurementExprs,
    geometryProcessingExprs,
    overlayExprs,
    affineTransformationExprs,
    linearReferencingExprs,
    spatialReferenceSystemExprs,
    geometryValidationExprs,
    boundingBoxExprs,
    spatialIndexingExprs,
    addressExprs,
    otherExprs,
    geographyExprs,
    rasterConstructorExprs,
    rasterAccessorExprs,
    rasterBandAccessorExprs,
    rasterOperatorExprs,
    rasterOutputExprs,
    rasterPredicateExprs,
    rasterGeometryExprs,
    pixelExprs,
    mapAlgebraExprs,
    rasterMapAlgebraExprs,
    rasterTileExprs,
    clusteringExprs,
    spatialStatisticsExprs)

  override val expressions: Seq[FunctionDescription] = categorizedSequences.flatten

  // lazy so the aggregator instances (which trigger Spark encoder lookups via SQLImplicits)
  // are only constructed when registerAll is called and Spark is set up. This lets the
  // categorization invariant test access `Catalog.expressions` without bootstrapping Spark.
  lazy val aggregateExpressions: Seq[Aggregator[Geometry, _, _]] =
    Seq(
      new ST_Envelope_Aggr,
      new ST_Extent,
      new ST_Intersection_Aggr,
      new ST_Union_Aggr(),
      new ST_Collect_Agg())
}
