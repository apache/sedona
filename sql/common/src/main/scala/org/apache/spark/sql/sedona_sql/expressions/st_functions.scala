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

import org.apache.spark.sql.Column
import org.apache.spark.sql.sedona_sql.expressions.collect.{ST_Collect}
import org.locationtech.jts.operation.buffer.BufferParameters

object st_functions extends DataFrameAPI {
  def ST_3DDistance(a: Column, b: Column): Column = wrapExpression[ST_3DDistance](a, b)
  def ST_3DDistance(a: String, b: String): Column = wrapExpression[ST_3DDistance](a, b)

  def ST_AddPoint(lineString: Column, point: Column): Column = wrapExpression[ST_AddPoint](lineString, point, -1)
  def ST_AddPoint(lineString: String, point: String): Column = wrapExpression[ST_AddPoint](lineString, point, -1)
  def ST_AddPoint(lineString: Column, point: Column, index: Column): Column = wrapExpression[ST_AddPoint](lineString, point, index)
  def ST_AddPoint(lineString: String, point: String, index: Int): Column = wrapExpression[ST_AddPoint](lineString, point, index)

  def ST_Area(geometry: Column): Column = wrapExpression[ST_Area](geometry)
  def ST_Area(geometry: String): Column = wrapExpression[ST_Area](geometry)

  def ST_AsBinary(geometry: Column): Column = wrapExpression[ST_AsBinary](geometry)
  def ST_AsBinary(geometry: String): Column = wrapExpression[ST_AsBinary](geometry)

  def ST_AsEWKB(geometry: Column): Column = wrapExpression[ST_AsEWKB](geometry)
  def ST_AsEWKB(geometry: String): Column = wrapExpression[ST_AsEWKB](geometry)

  def ST_AsEWKT(geometry: Column): Column = wrapExpression[ST_AsEWKT](geometry)
  def ST_AsEWKT(geometry: String): Column = wrapExpression[ST_AsEWKT](geometry)

  def ST_AsGeoJSON(geometry: Column): Column = wrapExpression[ST_AsGeoJSON](geometry)
  def ST_AsGeoJSON(geometry: String): Column = wrapExpression[ST_AsGeoJSON](geometry)

  def ST_AsGML(geometry: Column): Column = wrapExpression[ST_AsGML](geometry)
  def ST_AsGML(geometry: String): Column = wrapExpression[ST_AsGML](geometry)

  def ST_AsKML(geometry: Column): Column = wrapExpression[ST_AsKML](geometry)
  def ST_AsKML(geometry: String): Column = wrapExpression[ST_AsKML](geometry)

  def ST_AsText(geometry: Column): Column = wrapExpression[ST_AsText](geometry)
  def ST_AsText(geometry: String): Column = wrapExpression[ST_AsText](geometry)

  def ST_Azimuth(pointA: Column, pointB: Column): Column = wrapExpression[ST_Azimuth](pointA, pointB)
  def ST_Azimuth(pointA: String, pointB: String): Column = wrapExpression[ST_Azimuth](pointA, pointB)

  def ST_Boundary(geometry: Column): Column = wrapExpression[ST_Boundary](geometry)
  def ST_Boundary(geometry: String): Column = wrapExpression[ST_Boundary](geometry)

  def ST_Buffer(geometry: Column, buffer: Column): Column = wrapExpression[ST_Buffer](geometry, buffer)
  def ST_Buffer(geometry: String, buffer: Double): Column = wrapExpression[ST_Buffer](geometry, buffer)

  def ST_BuildArea(geometry: Column): Column = wrapExpression[ST_BuildArea](geometry)
  def ST_BuildArea(geometry: String): Column = wrapExpression[ST_BuildArea](geometry)

  def ST_Centroid(geometry: Column): Column = wrapExpression[ST_Centroid](geometry)
  def ST_Centroid(geometry: String): Column = wrapExpression[ST_Centroid](geometry)

  def ST_Collect(geoms: Column): Column = wrapExpression[ST_Collect](geoms)
  def ST_Collect(geoms: String): Column = wrapExpression[ST_Collect](geoms)
  def ST_Collect(geoms: Any*): Column = wrapVarArgExpression[ST_Collect](geoms)

  def ST_CollectionExtract(collection: Column): Column = wrapExpression[ST_CollectionExtract](collection, null)
  def ST_CollectionExtract(collection: String): Column = wrapExpression[ST_CollectionExtract](collection, null)
  def ST_CollectionExtract(collection: Column, geomType: Column): Column = wrapExpression[ST_CollectionExtract](collection, geomType)
  def ST_CollectionExtract(collection: String, geomType: Int): Column = wrapExpression[ST_CollectionExtract](collection, geomType)

  def ST_ConcaveHull(geometry: Column, pctConvex:Column): Column = wrapExpression[ST_ConcaveHull](geometry, pctConvex, false)
  def ST_ConcaveHull(geometry: String, pctConvex:Double): Column = wrapExpression[ST_ConcaveHull](geometry, pctConvex, false)
  def ST_ConcaveHull(geometry: Column, pctConvex:Column, allowHoles: Column): Column = wrapExpression[ST_ConcaveHull](geometry, pctConvex, allowHoles)
  def ST_ConcaveHull(geometry: String, pctConvex:Double, allowHoles: Boolean): Column = wrapExpression[ST_ConcaveHull](geometry, pctConvex, allowHoles)

  def ST_ConvexHull(geometry: Column): Column = wrapExpression[ST_ConvexHull](geometry)
  def ST_ConvexHull(geometry: String): Column = wrapExpression[ST_ConvexHull](geometry)

  def ST_Difference(a: Column, b: Column): Column = wrapExpression[ST_Difference](a, b)
  def ST_Difference(a: String, b: String): Column = wrapExpression[ST_Difference](a, b)

  def ST_Distance(a: Column, b: Column): Column = wrapExpression[ST_Distance](a, b)
  def ST_Distance(a: String, b: String): Column = wrapExpression[ST_Distance](a, b)

  def ST_Dump(geometry: Column): Column = wrapExpression[ST_Dump](geometry)
  def ST_Dump(geometry: String): Column = wrapExpression[ST_Dump](geometry)

  def ST_DumpPoints(geometry: Column): Column = wrapExpression[ST_DumpPoints](geometry)
  def ST_DumpPoints(geometry: String): Column = wrapExpression[ST_DumpPoints](geometry)

  def ST_EndPoint(lineString: Column): Column = wrapExpression[ST_EndPoint](lineString)
  def ST_EndPoint(lineString: String): Column = wrapExpression[ST_EndPoint](lineString)

  def ST_Envelope(geometry: Column): Column = wrapExpression[ST_Envelope](geometry)
  def ST_Envelope(geometry: String): Column = wrapExpression[ST_Envelope](geometry)

  def ST_ExteriorRing(polygon: Column): Column = wrapExpression[ST_ExteriorRing](polygon)
  def ST_ExteriorRing(polygon: String): Column = wrapExpression[ST_ExteriorRing](polygon)

  def ST_FlipCoordinates(geometry: Column): Column = wrapExpression[ST_FlipCoordinates](geometry)
  def ST_FlipCoordinates(geometry: String): Column = wrapExpression[ST_FlipCoordinates](geometry)

  def ST_Force_2D(geometry: Column): Column = wrapExpression[ST_Force_2D](geometry)
  def ST_Force_2D(geometry: String): Column = wrapExpression[ST_Force_2D](geometry)

  def ST_GeoHash(geometry: Column, precision: Column): Column = wrapExpression[ST_GeoHash](geometry, precision)
  def ST_GeoHash(geometry: String, precision: Int): Column = wrapExpression[ST_GeoHash](geometry, precision)

  def ST_GeometryN(multiGeometry: Column, n: Column): Column = wrapExpression[ST_GeometryN](multiGeometry, n)
  def ST_GeometryN(multiGeometry: String, n: Int): Column = wrapExpression[ST_GeometryN](multiGeometry, n)

  def ST_GeometryType(geometry: Column): Column = wrapExpression[ST_GeometryType](geometry)
  def ST_GeometryType(geometry: String): Column = wrapExpression[ST_GeometryType](geometry)

  def ST_InteriorRingN(polygon: Column, n: Column): Column = wrapExpression[ST_InteriorRingN](polygon, n)
  def ST_InteriorRingN(polygon: String, n: Int): Column = wrapExpression[ST_InteriorRingN](polygon, n)

  def ST_Intersection(a: Column, b: Column): Column = wrapExpression[ST_Intersection](a, b)
  def ST_Intersection(a: String, b: String): Column = wrapExpression[ST_Intersection](a, b)

  def ST_IsClosed(geometry: Column): Column = wrapExpression[ST_IsClosed](geometry)
  def ST_IsClosed(geometry: String): Column = wrapExpression[ST_IsClosed](geometry)

  def ST_IsEmpty(geometry: Column): Column = wrapExpression[ST_IsEmpty](geometry)
  def ST_IsEmpty(geometry: String): Column = wrapExpression[ST_IsEmpty](geometry)

  def ST_IsRing(lineString: Column): Column = wrapExpression[ST_IsRing](lineString)
  def ST_IsRing(lineString: String): Column = wrapExpression[ST_IsRing](lineString)

  def ST_IsSimple(geometry: Column): Column = wrapExpression[ST_IsSimple](geometry)
  def ST_IsSimple(geometry: String): Column = wrapExpression[ST_IsSimple](geometry)

  def ST_IsValid(geometry: Column): Column = wrapExpression[ST_IsValid](geometry)
  def ST_IsValid(geometry: String): Column = wrapExpression[ST_IsValid](geometry)

  def ST_Length(geometry: Column): Column = wrapExpression[ST_Length](geometry)
  def ST_Length(geometry: String): Column = wrapExpression[ST_Length](geometry)

  def ST_LineFromMultiPoint(geometry: Column): Column = wrapExpression[ST_LineFromMultiPoint](geometry)
  def ST_LineFromMultiPoint(geometry: String): Column = wrapExpression[ST_LineFromMultiPoint](geometry)

  def ST_LineInterpolatePoint(geometry: Column, fraction: Column): Column = wrapExpression[ST_LineInterpolatePoint](geometry, fraction)
  def ST_LineInterpolatePoint(geometry: String, fraction: Double): Column = wrapExpression[ST_LineInterpolatePoint](geometry, fraction)

  def ST_LineMerge(multiLineString: Column): Column = wrapExpression[ST_LineMerge](multiLineString)
  def ST_LineMerge(multiLineString: String): Column = wrapExpression[ST_LineMerge](multiLineString)

  def ST_LineSubstring(lineString: Column, startFraction: Column, endFraction: Column): Column = wrapExpression[ST_LineSubstring](lineString, startFraction, endFraction)
  def ST_LineSubstring(lineString: String, startFraction: Double, endFraction: Double): Column = wrapExpression[ST_LineSubstring](lineString, startFraction, endFraction)

  def ST_MakePolygon(lineString: Column): Column = wrapExpression[ST_MakePolygon](lineString, null)
  def ST_MakePolygon(lineString: String): Column = wrapExpression[ST_MakePolygon](lineString, null)
  def ST_MakePolygon(lineString: Column, holes: Column): Column = wrapExpression[ST_MakePolygon](lineString, holes)
  def ST_MakePolygon(lineString: String, holes: String): Column = wrapExpression[ST_MakePolygon](lineString, holes)

  def ST_MakeValid(geometry: Column): Column = wrapExpression[ST_MakeValid](geometry, false)
  def ST_MakeValid(geometry: String): Column = wrapExpression[ST_MakeValid](geometry, false)
  def ST_MakeValid(geometry: Column, keepCollapsed: Column): Column = wrapExpression[ST_MakeValid](geometry, keepCollapsed)
  def ST_MakeValid(geometry: String, keepCollapsed: Boolean): Column = wrapExpression[ST_MakeValid](geometry, keepCollapsed)

  def ST_MinimumBoundingCircle(geometry: Column): Column = wrapExpression[ST_MinimumBoundingCircle](geometry, BufferParameters.DEFAULT_QUADRANT_SEGMENTS)
  def ST_MinimumBoundingCircle(geometry: String): Column = wrapExpression[ST_MinimumBoundingCircle](geometry, BufferParameters.DEFAULT_QUADRANT_SEGMENTS)
  def ST_MinimumBoundingCircle(geometry: Column, quadrantSegments: Column): Column = wrapExpression[ST_MinimumBoundingCircle](geometry, quadrantSegments)
  def ST_MinimumBoundingCircle(geometry: String, quadrantSegments: Int): Column = wrapExpression[ST_MinimumBoundingCircle](geometry, quadrantSegments)

  def ST_MinimumBoundingRadius(geometry: Column): Column = wrapExpression[ST_MinimumBoundingRadius](geometry)
  def ST_MinimumBoundingRadius(geometry: String): Column = wrapExpression[ST_MinimumBoundingRadius](geometry)

  def ST_Multi(geometry: Column): Column = wrapExpression[ST_Multi](geometry)
  def ST_Multi(geometry: String): Column = wrapExpression[ST_Multi](geometry)

  def ST_Normalize(geometry: Column): Column = wrapExpression[ST_Normalize](geometry)
  def ST_Normalize(geometry: String): Column = wrapExpression[ST_Normalize](geometry)

  def ST_NPoints(geometry: Column): Column = wrapExpression[ST_NPoints](geometry)
  def ST_NPoints(geometry: String): Column = wrapExpression[ST_NPoints](geometry)

  def ST_NDims(geometry: Column): Column = wrapExpression[ST_NDims](geometry)
  def ST_NDims(geometry: String): Column = wrapExpression[ST_NDims](geometry)

  def ST_NumGeometries(geometry: Column): Column = wrapExpression[ST_NumGeometries](geometry)
  def ST_NumGeometries(geometry: String): Column = wrapExpression[ST_NumGeometries](geometry)

  def ST_NumInteriorRings(geometry: Column): Column = wrapExpression[ST_NumInteriorRings](geometry)
  def ST_NumInteriorRings(geometry: String): Column = wrapExpression[ST_NumInteriorRings](geometry)

  def ST_PointN(geometry: Column, n: Column): Column = wrapExpression[ST_PointN](geometry, n)
  def ST_PointN(geometry: String, n: Int): Column = wrapExpression[ST_PointN](geometry, n)

  def ST_PointOnSurface(geometry: Column): Column = wrapExpression[ST_PointOnSurface](geometry)
  def ST_PointOnSurface(geometry: String): Column = wrapExpression[ST_PointOnSurface](geometry)

  def ST_PrecisionReduce(geometry: Column, precision: Column): Column = wrapExpression[ST_PrecisionReduce](geometry, precision)
  def ST_PrecisionReduce(geometry: String, precision: Int): Column = wrapExpression[ST_PrecisionReduce](geometry, precision)

  def ST_RemovePoint(lineString: Column, index: Column): Column = wrapExpression[ST_RemovePoint](lineString, index)
  def ST_RemovePoint(lineString: String, index: Int): Column = wrapExpression[ST_RemovePoint](lineString, index)

  def ST_Reverse(geometry: Column): Column = wrapExpression[ST_Reverse](geometry)
  def ST_Reverse(geometry: String): Column = wrapExpression[ST_Reverse](geometry)

  def ST_S2CellIDs(geometry: Column, level: Column): Column = wrapExpression[ST_S2CellIDs](geometry, level)

  def ST_S2CellIDs(geometry: String, level: Int): Column = wrapExpression[ST_S2CellIDs](geometry, level)

  def ST_SetPoint(lineString: Column, index: Column, point: Column): Column = wrapExpression[ST_SetPoint](lineString, index, point)
  def ST_SetPoint(lineString: String, index: Int, point: String): Column = wrapExpression[ST_SetPoint](lineString, index, point)

  def ST_SetSRID(geometry: Column, srid: Column): Column = wrapExpression[ST_SetSRID](geometry, srid)
  def ST_SetSRID(geometry: String, srid: Int): Column = wrapExpression[ST_SetSRID](geometry, srid)

  def ST_SRID(geometry: Column): Column = wrapExpression[ST_SRID](geometry)
  def ST_SRID(geometry: String): Column = wrapExpression[ST_SRID](geometry)

  def ST_StartPoint(lineString: Column): Column = wrapExpression[ST_StartPoint](lineString)
  def ST_StartPoint(lineString: String): Column = wrapExpression[ST_StartPoint](lineString)

  def ST_SubDivide(geometry: Column, maxVertices: Column): Column = wrapExpression[ST_SubDivide](geometry, maxVertices)
  def ST_SubDivide(geometry: String, maxVertices: Int): Column = wrapExpression[ST_SubDivide](geometry, maxVertices)

  def ST_SubDivideExplode(geometry: Column, maxVertices: Column): Column = wrapExpression[ST_SubDivideExplode](geometry, maxVertices)
  def ST_SubDivideExplode(geometry: String, maxVertices: Int): Column = wrapExpression[ST_SubDivideExplode](geometry, maxVertices)

  def ST_SimplifyPreserveTopology(geometry: Column, distanceTolerance: Column): Column = wrapExpression[ST_SimplifyPreserveTopology](geometry, distanceTolerance)
  def ST_SimplifyPreserveTopology(geometry: String, distanceTolerance: Double): Column = wrapExpression[ST_SimplifyPreserveTopology](geometry, distanceTolerance)

  def ST_Split(input: Column, blade: Column): Column = wrapExpression[ST_Split](input, blade)
  def ST_Split(input: String, blade: String): Column = wrapExpression[ST_Split](input, blade)

  def ST_SymDifference(a: Column, b: Column): Column = wrapExpression[ST_SymDifference](a, b)
  def ST_SymDifference(a: String, b: String): Column = wrapExpression[ST_SymDifference](a, b)

  def ST_Transform(geometry: Column, sourceCRS: Column, targetCRS: Column): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, false)
  def ST_Transform(geometry: String, sourceCRS: String, targetCRS: String): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, false)
  def ST_Transform(geometry: Column, sourceCRS: Column, targetCRS: Column, disableError: Column): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, disableError)
  def ST_Transform(geometry: String, sourceCRS: String, targetCRS: String, disableError: Boolean): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, disableError)

  def ST_Union(a: Column, b: Column): Column = wrapExpression[ST_Union](a, b)
  def ST_Union(a: String, b: String): Column = wrapExpression[ST_Union](a, b)

  def ST_X(point: Column): Column = wrapExpression[ST_X](point)
  def ST_X(point: String): Column = wrapExpression[ST_X](point)

  def ST_XMax(geometry: Column): Column = wrapExpression[ST_XMax](geometry)
  def ST_XMax(geometry: String): Column = wrapExpression[ST_XMax](geometry)

  def ST_XMin(geometry: Column): Column = wrapExpression[ST_XMin](geometry)
  def ST_XMin(geometry: String): Column = wrapExpression[ST_XMin](geometry)

  def ST_Y(point: Column): Column = wrapExpression[ST_Y](point)
  def ST_Y(point: String): Column = wrapExpression[ST_Y](point)

  def ST_YMax(geometry: Column): Column = wrapExpression[ST_YMax](geometry)
  def ST_YMax(geometry: String): Column = wrapExpression[ST_YMax](geometry)

  def ST_YMin(geometry: Column): Column = wrapExpression[ST_YMin](geometry)
  def ST_YMin(geometry: String): Column = wrapExpression[ST_YMin](geometry)

  def ST_Z(point: Column): Column = wrapExpression[ST_Z](point)
  def ST_Z(point: String): Column = wrapExpression[ST_Z](point)

  def ST_ZMax(geometry: Column): Column = wrapExpression[ST_ZMax](geometry)
  def ST_ZMax(geometry: String): Column = wrapExpression[ST_ZMax](geometry)

  def ST_ZMin(geometry: Column): Column = wrapExpression[ST_ZMin](geometry)
  def ST_ZMin(geometry: String): Column = wrapExpression[ST_ZMin](geometry)
}
