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
import org.apache.spark.sql.sedona_sql.expressions.collect.ST_Collect
import org.locationtech.jts.operation.buffer.BufferParameters

object st_functions extends DataFrameAPI {
  def GeometryType(geometry: Column): Column = wrapExpression[GeometryType](geometry)
  def GeometryType(geometry: String): Column = wrapExpression[GeometryType](geometry)

  def ST_3DDistance(a: Column, b: Column): Column = wrapExpression[ST_3DDistance](a, b)
  def ST_3DDistance(a: String, b: String): Column = wrapExpression[ST_3DDistance](a, b)

  def ST_AddMeasure(geom: Column, measureStart: Column, measureEnd: Column): Column =
    wrapExpression[ST_AddMeasure](geom, measureStart, measureEnd)
  def ST_AddMeasure(geom: String, measureStart: Double, measureEnd: Double): Column =
    wrapExpression[ST_AddMeasure](geom, measureStart, measureEnd)
  def ST_AddMeasure(geom: String, measureStart: String, measureEnd: String): Column =
    wrapExpression[ST_AddMeasure](geom, measureStart, measureEnd)

  def ST_AddPoint(lineString: Column, point: Column): Column =
    wrapExpression[ST_AddPoint](lineString, point, -1)
  def ST_AddPoint(lineString: String, point: String): Column =
    wrapExpression[ST_AddPoint](lineString, point, -1)
  def ST_AddPoint(lineString: Column, point: Column, index: Column): Column =
    wrapExpression[ST_AddPoint](lineString, point, index)
  def ST_AddPoint(lineString: String, point: String, index: Int): Column =
    wrapExpression[ST_AddPoint](lineString, point, index)

  def ST_Area(geometry: Column): Column = wrapExpression[ST_Area](geometry)
  def ST_Area(geometry: String): Column = wrapExpression[ST_Area](geometry)

  def ST_AsBinary(geometry: Column): Column = wrapExpression[ST_AsBinary](geometry)
  def ST_AsBinary(geometry: String): Column = wrapExpression[ST_AsBinary](geometry)

  def ST_AsEWKB(geometry: Column): Column = wrapExpression[ST_AsEWKB](geometry)
  def ST_AsEWKB(geometry: String): Column = wrapExpression[ST_AsEWKB](geometry)

  def ST_AsHEXEWKB(geometry: Column, endian: Column): Column =
    wrapExpression[ST_AsHEXEWKB](geometry, endian)
  def ST_AsHEXEWKB(geometry: String, endian: String): Column =
    wrapExpression[ST_AsHEXEWKB](geometry, endian)
  def ST_AsHEXEWKB(geometry: Column): Column = wrapExpression[ST_AsHEXEWKB](geometry)
  def ST_AsHEXEWKB(geometry: String): Column = wrapExpression[ST_AsHEXEWKB](geometry)

  def ST_AsEWKT(geometry: Column): Column = wrapExpression[ST_AsEWKT](geometry)
  def ST_AsEWKT(geometry: String): Column = wrapExpression[ST_AsEWKT](geometry)

  def ST_AsGeoJSON(geometry: Column): Column = wrapExpression[ST_AsGeoJSON](geometry)
  def ST_AsGeoJSON(geometry: String): Column = wrapExpression[ST_AsGeoJSON](geometry)

  def ST_AsGeoJSON(geometry: Column, Type: Column): Column =
    wrapExpression[ST_AsGeoJSON](geometry, Type)
  def ST_AsGeoJSON(geometry: String, Type: String): Column =
    wrapExpression[ST_AsGeoJSON](geometry, Type)

  def ST_AsGML(geometry: Column): Column = wrapExpression[ST_AsGML](geometry)
  def ST_AsGML(geometry: String): Column = wrapExpression[ST_AsGML](geometry)

  def ST_AsKML(geometry: Column): Column = wrapExpression[ST_AsKML](geometry)
  def ST_AsKML(geometry: String): Column = wrapExpression[ST_AsKML](geometry)

  def ST_AsText(geometry: Column): Column = wrapExpression[ST_AsText](geometry)
  def ST_AsText(geometry: String): Column = wrapExpression[ST_AsText](geometry)

  def ST_Azimuth(pointA: Column, pointB: Column): Column =
    wrapExpression[ST_Azimuth](pointA, pointB)
  def ST_Azimuth(pointA: String, pointB: String): Column =
    wrapExpression[ST_Azimuth](pointA, pointB)

  def ST_Boundary(geometry: Column): Column = wrapExpression[ST_Boundary](geometry)
  def ST_Boundary(geometry: String): Column = wrapExpression[ST_Boundary](geometry)

  def ST_Buffer(geometry: Column, buffer: Column): Column =
    wrapExpression[ST_Buffer](geometry, buffer)
  def ST_Buffer(geometry: String, buffer: Double): Column =
    wrapExpression[ST_Buffer](geometry, buffer)
  def ST_Buffer(geometry: Column, buffer: Column, useSpheroid: Column): Column =
    wrapExpression[ST_Buffer](geometry, buffer, useSpheroid)
  def ST_Buffer(geometry: String, buffer: Double, useSpheroid: Boolean): Column =
    wrapExpression[ST_Buffer](geometry, buffer, useSpheroid)
  def ST_Buffer(
      geometry: Column,
      buffer: Column,
      useSpheroid: Column,
      parameters: Column): Column =
    wrapExpression[ST_Buffer](geometry, buffer, useSpheroid, parameters)
  def ST_Buffer(
      geometry: String,
      buffer: Double,
      useSpheroid: Boolean,
      parameters: String): Column =
    wrapExpression[ST_Buffer](geometry, buffer, useSpheroid, parameters)

  def ST_BestSRID(geometry: Column): Column = wrapExpression[ST_BestSRID](geometry)
  def ST_BestSRID(geometry: String): Column = wrapExpression[ST_BestSRID](geometry)

  def ST_ShiftLongitude(geometry: Column): Column = wrapExpression[ST_ShiftLongitude](geometry)
  def ST_ShiftLongitude(geometry: String): Column = wrapExpression[ST_ShiftLongitude](geometry)

  def ST_BuildArea(geometry: Column): Column = wrapExpression[ST_BuildArea](geometry)
  def ST_BuildArea(geometry: String): Column = wrapExpression[ST_BuildArea](geometry)

  def ST_Centroid(geometry: Column): Column = wrapExpression[ST_Centroid](geometry)
  def ST_Centroid(geometry: String): Column = wrapExpression[ST_Centroid](geometry)

  def ST_ClosestPoint(a: Column, b: Column): Column = wrapExpression[ST_ClosestPoint](a, b)
  def ST_ClosestPoint(a: String, b: String): Column = wrapExpression[ST_ClosestPoint](a, b)

  def ST_Collect(geoms: Column): Column = wrapExpression[ST_Collect](geoms)
  def ST_Collect(geoms: String): Column = wrapExpression[ST_Collect](geoms)
  def ST_Collect(geoms: Any*): Column = wrapVarArgExpression[ST_Collect](geoms)

  def ST_CollectionExtract(collection: Column): Column =
    wrapExpression[ST_CollectionExtract](collection, null)
  def ST_CollectionExtract(collection: String): Column =
    wrapExpression[ST_CollectionExtract](collection, null)
  def ST_CollectionExtract(collection: Column, geomType: Column): Column =
    wrapExpression[ST_CollectionExtract](collection, geomType)
  def ST_CollectionExtract(collection: String, geomType: Int): Column =
    wrapExpression[ST_CollectionExtract](collection, geomType)

  def ST_ConcaveHull(geometry: Column, pctConvex: Column): Column =
    wrapExpression[ST_ConcaveHull](geometry, pctConvex, false)
  def ST_ConcaveHull(geometry: String, pctConvex: Double): Column =
    wrapExpression[ST_ConcaveHull](geometry, pctConvex, false)
  def ST_ConcaveHull(geometry: Column, pctConvex: Column, allowHoles: Column): Column =
    wrapExpression[ST_ConcaveHull](geometry, pctConvex, allowHoles)
  def ST_ConcaveHull(geometry: String, pctConvex: Double, allowHoles: Boolean): Column =
    wrapExpression[ST_ConcaveHull](geometry, pctConvex, allowHoles)

  def ST_ConvexHull(geometry: Column): Column = wrapExpression[ST_ConvexHull](geometry)
  def ST_ConvexHull(geometry: String): Column = wrapExpression[ST_ConvexHull](geometry)

  def ST_CrossesDateLine(a: Column): Column = wrapExpression[ST_CrossesDateLine](a)
  def ST_CrossesDateLine(a: String): Column = wrapExpression[ST_CrossesDateLine](a)

  def ST_Difference(a: Column, b: Column): Column = wrapExpression[ST_Difference](a, b)
  def ST_Difference(a: String, b: String): Column = wrapExpression[ST_Difference](a, b)

  def ST_Dimension(geometry: Column): Column = wrapExpression[ST_Dimension](geometry)
  def ST_Dimension(geometry: String): Column = wrapExpression[ST_Dimension](geometry)

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

  def ST_Expand(geometry: Column, uniformDelta: Column) =
    wrapExpression[ST_Expand](geometry, uniformDelta)
  def ST_Expand(geometry: String, uniformDelta: String) =
    wrapExpression[ST_Expand](geometry, uniformDelta)
  def ST_Expand(geometry: String, uniformDelta: Double) =
    wrapExpression[ST_Expand](geometry, uniformDelta)
  def ST_Expand(geometry: Column, deltaX: Column, deltaY: Column) =
    wrapExpression[ST_Expand](geometry, deltaX, deltaY)
  def ST_Expand(geometry: String, deltaX: String, deltaY: String) =
    wrapExpression[ST_Expand](geometry, deltaX, deltaY)
  def ST_Expand(geometry: String, deltaX: Double, deltaY: Double) =
    wrapExpression[ST_Expand](geometry, deltaX, deltaY)
  def ST_Expand(geometry: Column, deltaX: Column, deltaY: Column, deltaZ: Column) =
    wrapExpression[ST_Expand](geometry, deltaX, deltaY, deltaZ)
  def ST_Expand(geometry: String, deltaX: String, deltaY: String, deltaZ: String) =
    wrapExpression[ST_Expand](geometry, deltaX, deltaY, deltaZ)
  def ST_Expand(geometry: String, deltaX: Double, deltaY: Double, deltaZ: Double) =
    wrapExpression[ST_Expand](geometry, deltaX, deltaY, deltaZ)

  def ST_ExteriorRing(polygon: Column): Column = wrapExpression[ST_ExteriorRing](polygon)
  def ST_ExteriorRing(polygon: String): Column = wrapExpression[ST_ExteriorRing](polygon)

  def ST_FlipCoordinates(geometry: Column): Column = wrapExpression[ST_FlipCoordinates](geometry)
  def ST_FlipCoordinates(geometry: String): Column = wrapExpression[ST_FlipCoordinates](geometry)

  def ST_Force_2D(geometry: Column): Column = wrapExpression[ST_Force_2D](geometry)
  def ST_Force_2D(geometry: String): Column = wrapExpression[ST_Force_2D](geometry)

  def ST_GeoHash(geometry: Column, precision: Column): Column =
    wrapExpression[ST_GeoHash](geometry, precision)
  def ST_GeoHash(geometry: String, precision: Int): Column =
    wrapExpression[ST_GeoHash](geometry, precision)

  def ST_GeometryN(multiGeometry: Column, n: Column): Column =
    wrapExpression[ST_GeometryN](multiGeometry, n)
  def ST_GeometryN(multiGeometry: String, n: Int): Column =
    wrapExpression[ST_GeometryN](multiGeometry, n)

  def ST_GeometryType(geometry: Column): Column = wrapExpression[ST_GeometryType](geometry)
  def ST_GeometryType(geometry: String): Column = wrapExpression[ST_GeometryType](geometry)

  def ST_H3CellDistance(cell1: Column, cell2: Column): Column =
    wrapExpression[ST_H3CellDistance](cell1, cell2)

  def ST_H3CellDistance(cell1: Long, cell2: Long): Column =
    wrapExpression[ST_H3CellDistance](cell1, cell2)
  def ST_H3CellIDs(geometry: Column, level: Column, fullCover: Column): Column =
    wrapExpression[ST_H3CellIDs](geometry, level, fullCover)

  def ST_H3CellIDs(geometry: String, level: Int, fullCover: Boolean): Column =
    wrapExpression[ST_H3CellIDs](geometry, level, fullCover)

  def ST_H3KRing(cell: Column, k: Column, exactRing: Column): Column =
    wrapExpression[ST_H3KRing](cell, k, exactRing)
  def ST_H3KRing(cell: Column, k: Integer, exactRing: Boolean): Column =
    wrapExpression[ST_H3KRing](cell, k, exactRing)

  def ST_H3KRing(cell: Long, k: Integer, exactRing: Boolean): Column =
    wrapExpression[ST_H3KRing](cell, k, exactRing)

  def ST_H3ToGeom(cellIds: Column): Column = wrapExpression[ST_H3ToGeom](cellIds)

  def ST_H3ToGeom(cellIds: Array[Long]): Column = wrapExpression[ST_H3ToGeom](cellIds)

  def ST_InteriorRingN(polygon: Column, n: Column): Column =
    wrapExpression[ST_InteriorRingN](polygon, n)
  def ST_InteriorRingN(polygon: String, n: Int): Column =
    wrapExpression[ST_InteriorRingN](polygon, n)

  def ST_Intersection(a: Column, b: Column): Column = wrapExpression[ST_Intersection](a, b)
  def ST_Intersection(a: String, b: String): Column = wrapExpression[ST_Intersection](a, b)

  def ST_IsClosed(geometry: Column): Column = wrapExpression[ST_IsClosed](geometry)
  def ST_IsClosed(geometry: String): Column = wrapExpression[ST_IsClosed](geometry)

  def ST_IsEmpty(geometry: Column): Column = wrapExpression[ST_IsEmpty](geometry)
  def ST_IsEmpty(geometry: String): Column = wrapExpression[ST_IsEmpty](geometry)

  def ST_IsPolygonCW(geometry: Column): Column = wrapExpression[ST_IsPolygonCW](geometry)
  def ST_IsPolygonCW(geometry: String): Column = wrapExpression[ST_IsPolygonCW](geometry)

  def ST_IsRing(lineString: Column): Column = wrapExpression[ST_IsRing](lineString)
  def ST_IsRing(lineString: String): Column = wrapExpression[ST_IsRing](lineString)

  def ST_IsSimple(geometry: Column): Column = wrapExpression[ST_IsSimple](geometry)
  def ST_IsSimple(geometry: String): Column = wrapExpression[ST_IsSimple](geometry)

  def ST_IsValidTrajectory(geometry: Column): Column =
    wrapExpression[ST_IsValidTrajectory](geometry)
  def ST_IsValidTrajectory(geometry: String): Column =
    wrapExpression[ST_IsValidTrajectory](geometry)

  def ST_IsValid(geometry: Column): Column = wrapExpression[ST_IsValid](geometry)
  def ST_IsValid(geometry: String): Column = wrapExpression[ST_IsValid](geometry)

  def ST_IsValid(geometry: Column, flag: Column): Column =
    wrapExpression[ST_IsValid](geometry, flag)
  def ST_IsValid(geometry: String, flag: Integer): Column =
    wrapExpression[ST_IsValid](geometry, flag)

  def ST_IsValidReason(geometry: Column): Column = wrapExpression[ST_IsValidReason](geometry)
  def ST_IsValidReason(geometry: String): Column = wrapExpression[ST_IsValidReason](geometry)

  def ST_IsValidReason(geometry: Column, flag: Column): Column =
    wrapExpression[ST_IsValidReason](geometry, flag)
  def ST_IsValidReason(geometry: String, flag: Integer): Column =
    wrapExpression[ST_IsValidReason](geometry, flag)

  def ST_IsValidDetail(geometry: Column, flag: Column): Column =
    wrapExpression[ST_IsValidDetail](geometry, flag)
  def ST_IsValidDetail(geometry: String, flag: Integer): Column =
    wrapExpression[ST_IsValidDetail](geometry, flag)
  def ST_IsValidDetail(geometry: String, flag: String): Column =
    wrapExpression[ST_IsValidDetail](geometry, flag)
  def ST_IsValidDetail(geometry: Column): Column = wrapExpression[ST_IsValidDetail](geometry)
  def ST_IsValidDetail(geometry: String): Column = wrapExpression[ST_IsValidDetail](geometry)

  def ST_Length(geometry: Column): Column = wrapExpression[ST_Length](geometry)
  def ST_Length(geometry: String): Column = wrapExpression[ST_Length](geometry)

  def ST_Length2D(geometry: Column): Column = wrapExpression[ST_Length2D](geometry)
  def ST_Length2D(geometry: String): Column = wrapExpression[ST_Length2D](geometry)

  def ST_LineFromMultiPoint(geometry: Column): Column =
    wrapExpression[ST_LineFromMultiPoint](geometry)
  def ST_LineFromMultiPoint(geometry: String): Column =
    wrapExpression[ST_LineFromMultiPoint](geometry)

  def ST_LineInterpolatePoint(geometry: Column, fraction: Column): Column =
    wrapExpression[ST_LineInterpolatePoint](geometry, fraction)
  def ST_LineInterpolatePoint(geometry: String, fraction: Double): Column =
    wrapExpression[ST_LineInterpolatePoint](geometry, fraction)

  def ST_LineLocatePoint(linestring: Column, point: Column): Column =
    wrapExpression[ST_LineLocatePoint](linestring, point)
  def ST_LineLocatePoint(linestring: String, point: String): Column =
    wrapExpression[ST_LineLocatePoint](linestring, point)

  def ST_LineMerge(multiLineString: Column): Column =
    wrapExpression[ST_LineMerge](multiLineString)
  def ST_LineMerge(multiLineString: String): Column =
    wrapExpression[ST_LineMerge](multiLineString)

  def ST_LineSubstring(lineString: Column, startFraction: Column, endFraction: Column): Column =
    wrapExpression[ST_LineSubstring](lineString, startFraction, endFraction)
  def ST_LineSubstring(lineString: String, startFraction: Double, endFraction: Double): Column =
    wrapExpression[ST_LineSubstring](lineString, startFraction, endFraction)

  def ST_LongestLine(geom1: Column, geom2: Column): Column =
    wrapExpression[ST_LongestLine](geom1, geom2)
  def ST_LongestLine(geom1: String, geom2: String): Column =
    wrapExpression[ST_LongestLine](geom1, geom2)

  def ST_LocateAlong(geom: Column, measure: Column, offset: Column): Column =
    wrapExpression[ST_LocateAlong](geom, measure, offset)
  def ST_LocateAlong(geom: String, measure: Double, offset: Double): Column =
    wrapExpression[ST_LocateAlong](geom, measure, offset)
  def ST_LocateAlong(geom: Column, measure: Column): Column =
    wrapExpression[ST_LocateAlong](geom, measure)
  def ST_LocateAlong(geom: String, measure: Double): Column =
    wrapExpression[ST_LocateAlong](geom, measure)

  def ST_HasZ(geoms: Column): Column = wrapExpression[ST_HasZ](geoms)
  def ST_HasZ(geoms: String): Column = wrapExpression[ST_HasZ](geoms)

  def ST_HasM(geoms: Column): Column = wrapExpression[ST_HasM](geoms)
  def ST_HasM(geoms: String): Column = wrapExpression[ST_HasM](geoms)

  def ST_M(geoms: Column): Column = wrapExpression[ST_M](geoms)
  def ST_M(geoms: String): Column = wrapExpression[ST_M](geoms)

  def ST_MMin(geoms: Column): Column = wrapExpression[ST_MMin](geoms)
  def ST_MMin(geoms: String): Column = wrapExpression[ST_MMin](geoms)

  def ST_MMax(geoms: Column): Column = wrapExpression[ST_MMax](geoms)
  def ST_MMax(geoms: String): Column = wrapExpression[ST_MMax](geoms)

  def ST_MakeLine(geoms: Column): Column = wrapExpression[ST_MakeLine](geoms)
  def ST_MakeLine(geoms: String): Column = wrapExpression[ST_MakeLine](geoms)
  def ST_MakeLine(geom1: Column, geom2: Column): Column =
    wrapExpression[ST_MakeLine](geom1, geom2)
  def ST_MakeLine(geom1: String, geom2: String): Column =
    wrapExpression[ST_MakeLine](geom1, geom2)

  def ST_Points(geom: Column): Column = wrapExpression[ST_Points](geom)
  def ST_Points(geom: String): Column = wrapExpression[ST_Points](geom)

  def ST_Polygon(lineString: Column, srid: Column): Column =
    wrapExpression[ST_Polygon](lineString, srid)
  def ST_Polygon(lineString: String, srid: Integer): Column =
    wrapExpression[ST_Polygon](lineString, srid)

  def ST_Polygonize(geoms: Column): Column = wrapExpression[ST_Polygonize](geoms)
  def ST_Polygonize(geoms: String): Column = wrapExpression[ST_Polygonize](geoms)

  def ST_MakePolygon(lineString: Column): Column =
    wrapExpression[ST_MakePolygon](lineString, null)
  def ST_MakePolygon(lineString: String): Column =
    wrapExpression[ST_MakePolygon](lineString, null)
  def ST_MakePolygon(lineString: Column, holes: Column): Column =
    wrapExpression[ST_MakePolygon](lineString, holes)
  def ST_MakePolygon(lineString: String, holes: String): Column =
    wrapExpression[ST_MakePolygon](lineString, holes)

  def ST_MakeValid(geometry: Column): Column = wrapExpression[ST_MakeValid](geometry, false)
  def ST_MakeValid(geometry: String): Column = wrapExpression[ST_MakeValid](geometry, false)
  def ST_MakeValid(geometry: Column, keepCollapsed: Column): Column =
    wrapExpression[ST_MakeValid](geometry, keepCollapsed)
  def ST_MakeValid(geometry: String, keepCollapsed: Boolean): Column =
    wrapExpression[ST_MakeValid](geometry, keepCollapsed)

  def ST_MaximumInscribedCircle(geometry: Column): Column =
    wrapExpression[ST_MaximumInscribedCircle](geometry)
  def ST_MaximumInscribedCircle(geometry: String): Column =
    wrapExpression[ST_MaximumInscribedCircle](geometry)

  def ST_MaxDistance(geom1: Column, geom2: Column): Column =
    wrapExpression[ST_MaxDistance](geom1, geom2)
  def ST_MaxDistance(geom1: String, geom2: String): Column =
    wrapExpression[ST_MaxDistance](geom1, geom2)

  def ST_MinimumClearance(geometry: Column): Column =
    wrapExpression[ST_MinimumClearance](geometry)
  def ST_MinimumClearance(geometry: String): Column =
    wrapExpression[ST_MinimumClearance](geometry)

  def ST_MinimumClearanceLine(geometry: Column): Column =
    wrapExpression[ST_MinimumClearanceLine](geometry)
  def ST_MinimumClearanceLine(geometry: String): Column =
    wrapExpression[ST_MinimumClearanceLine](geometry)

  def ST_MinimumBoundingCircle(geometry: Column): Column =
    wrapExpression[ST_MinimumBoundingCircle](
      geometry,
      BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6)
  def ST_MinimumBoundingCircle(geometry: String): Column =
    wrapExpression[ST_MinimumBoundingCircle](
      geometry,
      BufferParameters.DEFAULT_QUADRANT_SEGMENTS * 6)
  def ST_MinimumBoundingCircle(geometry: Column, quadrantSegments: Column): Column =
    wrapExpression[ST_MinimumBoundingCircle](geometry, quadrantSegments)
  def ST_MinimumBoundingCircle(geometry: String, quadrantSegments: Int): Column =
    wrapExpression[ST_MinimumBoundingCircle](geometry, quadrantSegments)

  def ST_MinimumBoundingRadius(geometry: Column): Column =
    wrapExpression[ST_MinimumBoundingRadius](geometry)
  def ST_MinimumBoundingRadius(geometry: String): Column =
    wrapExpression[ST_MinimumBoundingRadius](geometry)

  def ST_IsPolygonCCW(geometry: Column): Column = wrapExpression[ST_IsPolygonCCW](geometry)
  def ST_IsPolygonCCW(geometry: String): Column = wrapExpression[ST_IsPolygonCCW](geometry)

  def ST_ForcePolygonCCW(geometry: Column): Column = wrapExpression[ST_ForcePolygonCCW](geometry)
  def ST_ForcePolygonCCW(geometry: String): Column = wrapExpression[ST_ForcePolygonCCW](geometry)

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

  def ST_NumInteriorRings(geometry: Column): Column =
    wrapExpression[ST_NumInteriorRings](geometry)
  def ST_NumInteriorRings(geometry: String): Column =
    wrapExpression[ST_NumInteriorRings](geometry)

  def ST_NumInteriorRing(geometry: Column): Column = wrapExpression[ST_NumInteriorRing](geometry)
  def ST_NumInteriorRing(geometry: String): Column = wrapExpression[ST_NumInteriorRing](geometry)

  def ST_PointN(geometry: Column, n: Column): Column = wrapExpression[ST_PointN](geometry, n)
  def ST_PointN(geometry: String, n: Int): Column = wrapExpression[ST_PointN](geometry, n)

  def ST_PointOnSurface(geometry: Column): Column = wrapExpression[ST_PointOnSurface](geometry)
  def ST_PointOnSurface(geometry: String): Column = wrapExpression[ST_PointOnSurface](geometry)

  def ST_ReducePrecision(geometry: Column, precision: Column): Column =
    wrapExpression[ST_ReducePrecision](geometry, precision)
  def ST_ReducePrecision(geometry: String, precision: Int): Column =
    wrapExpression[ST_ReducePrecision](geometry, precision)

  def ST_RemovePoint(lineString: Column, index: Column): Column =
    wrapExpression[ST_RemovePoint](lineString, index)
  def ST_RemovePoint(lineString: String, index: Int): Column =
    wrapExpression[ST_RemovePoint](lineString, index)

  def ST_Reverse(geometry: Column): Column = wrapExpression[ST_Reverse](geometry)
  def ST_Reverse(geometry: String): Column = wrapExpression[ST_Reverse](geometry)

  def ST_RotateX(geometry: Column, angle: Column): Column =
    wrapExpression[ST_RotateX](geometry, angle)
  def ST_RotateX(geometry: String, angle: Double): Column =
    wrapExpression[ST_RotateX](geometry, angle)
  def ST_RotateX(geometry: String, angle: String): Column =
    wrapExpression[ST_RotateX](geometry, angle)

  def ST_Rotate(geometry: Column, angle: Column): Column =
    wrapExpression[ST_Rotate](geometry, angle)
  def ST_Rotate(geometry: String, angle: Double): Column =
    wrapExpression[ST_Rotate](geometry, angle)

  def ST_Rotate(geometry: Column, angle: Column, pointOrigin: Column): Column =
    wrapExpression[ST_Rotate](geometry, angle, pointOrigin)
  def ST_Rotate(geometry: String, angle: Double, pointOrigin: String): Column =
    wrapExpression[ST_Rotate](geometry, angle, pointOrigin)

  def ST_Rotate(geometry: Column, angle: Column, originX: Column, originY: Column): Column =
    wrapExpression[ST_Rotate](geometry, angle, originX, originY)
  def ST_Rotate(geometry: String, angle: Double, originX: Double, originY: Double): Column =
    wrapExpression[ST_Rotate](geometry, angle, originX, originY)

  def ST_S2CellIDs(geometry: Column, level: Column): Column =
    wrapExpression[ST_S2CellIDs](geometry, level)

  def ST_S2CellIDs(geometry: String, level: Int): Column =
    wrapExpression[ST_S2CellIDs](geometry, level)

  def ST_S2ToGeom(cellIDs: Column): Column = wrapExpression[ST_S2ToGeom](cellIDs)
  def ST_S2ToGeom(cellIDs: Array[Long]): Column = wrapExpression[ST_S2ToGeom](cellIDs)

  def ST_SetPoint(lineString: Column, index: Column, point: Column): Column =
    wrapExpression[ST_SetPoint](lineString, index, point)
  def ST_SetPoint(lineString: String, index: Int, point: String): Column =
    wrapExpression[ST_SetPoint](lineString, index, point)

  def ST_SetSRID(geometry: Column, srid: Column): Column =
    wrapExpression[ST_SetSRID](geometry, srid)
  def ST_SetSRID(geometry: String, srid: Int): Column = wrapExpression[ST_SetSRID](geometry, srid)

  def ST_SRID(geometry: Column): Column = wrapExpression[ST_SRID](geometry)
  def ST_SRID(geometry: String): Column = wrapExpression[ST_SRID](geometry)

  def ST_StartPoint(lineString: Column): Column = wrapExpression[ST_StartPoint](lineString)
  def ST_StartPoint(lineString: String): Column = wrapExpression[ST_StartPoint](lineString)

  def ST_Snap(input: Column, reference: Column, tolerance: Column): Column =
    wrapExpression[ST_Snap](input, reference, tolerance)
  def ST_Snap(input: String, reference: String, tolerance: Double): Column =
    wrapExpression[ST_Snap](input, reference, tolerance)

  def ST_SubDivide(geometry: Column, maxVertices: Column): Column =
    wrapExpression[ST_SubDivide](geometry, maxVertices)
  def ST_SubDivide(geometry: String, maxVertices: Int): Column =
    wrapExpression[ST_SubDivide](geometry, maxVertices)

  def ST_SubDivideExplode(geometry: Column, maxVertices: Column): Column =
    wrapExpression[ST_SubDivideExplode](geometry, maxVertices)
  def ST_SubDivideExplode(geometry: String, maxVertices: Int): Column =
    wrapExpression[ST_SubDivideExplode](geometry, maxVertices)

  def ST_SimplifyPreserveTopology(geometry: Column, distanceTolerance: Column): Column =
    wrapExpression[ST_SimplifyPreserveTopology](geometry, distanceTolerance)
  def ST_SimplifyPreserveTopology(geometry: String, distanceTolerance: Double): Column =
    wrapExpression[ST_SimplifyPreserveTopology](geometry, distanceTolerance)

  def ST_Split(input: Column, blade: Column): Column = wrapExpression[ST_Split](input, blade)
  def ST_Split(input: String, blade: String): Column = wrapExpression[ST_Split](input, blade)

  def ST_SymDifference(a: Column, b: Column): Column = wrapExpression[ST_SymDifference](a, b)
  def ST_SymDifference(a: String, b: String): Column = wrapExpression[ST_SymDifference](a, b)

  def ST_Transform(geometry: Column, sourceCRS: Column, targetCRS: Column): Column =
    wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, false)
  def ST_Transform(geometry: String, sourceCRS: String, targetCRS: String): Column =
    wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, false)
  def ST_Transform(
      geometry: Column,
      sourceCRS: Column,
      targetCRS: Column,
      disableError: Column): Column =
    wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, disableError)
  def ST_Transform(
      geometry: String,
      sourceCRS: String,
      targetCRS: String,
      disableError: Boolean): Column =
    wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, disableError)
  def ST_Transform(geometry: String, targetCRS: String): Column =
    wrapExpression[ST_Transform](geometry, targetCRS)
  def ST_Transform(geometry: Column, targetCRS: Column): Column =
    wrapExpression[ST_Transform](geometry, targetCRS)

  def ST_SimplifyVW(geometry: Column, distanceTolerance: Column): Column =
    wrapExpression[ST_SimplifyVW](geometry, distanceTolerance)
  def ST_SimplifyVW(geometry: String, distanceTolerance: Double): Column =
    wrapExpression[ST_SimplifyVW](geometry, distanceTolerance)

  def ST_SimplifyPolygonHull(geometry: Column, vertexFactor: Column): Column =
    wrapExpression[ST_SimplifyPolygonHull](geometry, vertexFactor)
  def ST_SimplifyPolygonHull(geometry: String, vertexFactor: Double): Column =
    wrapExpression[ST_SimplifyPolygonHull](geometry, vertexFactor)
  def ST_SimplifyPolygonHull(geometry: Column, vertexFactor: Column, isOuter: Column): Column =
    wrapExpression[ST_SimplifyPolygonHull](geometry, vertexFactor, isOuter)
  def ST_SimplifyPolygonHull(geometry: String, vertexFactor: Double, isOuter: Boolean): Column =
    wrapExpression[ST_SimplifyPolygonHull](geometry, vertexFactor, isOuter)

  def ST_UnaryUnion(geometry: Column): Column = wrapExpression[ST_UnaryUnion](geometry)
  def ST_UnaryUnion(geometry: String): Column = wrapExpression[ST_UnaryUnion](geometry)

  def ST_Union(a: Column, b: Column): Column = wrapExpression[ST_Union](a, b)
  def ST_Union(a: String, b: String): Column = wrapExpression[ST_Union](a, b)
  def ST_Union(geoms: Column): Column = wrapExpression[ST_Union](geoms)
  def ST_Union(geoms: String): Column = wrapExpression[ST_Union](geoms)

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

  def ST_Zmflag(geom: Column): Column = wrapExpression[ST_Zmflag](geom)
  def ST_Zmflag(geom: String): Column = wrapExpression[ST_Zmflag](geom)

  def ST_ZMax(geometry: Column): Column = wrapExpression[ST_ZMax](geometry)
  def ST_ZMax(geometry: String): Column = wrapExpression[ST_ZMax](geometry)

  def ST_ZMin(geometry: Column): Column = wrapExpression[ST_ZMin](geometry)
  def ST_ZMin(geometry: String): Column = wrapExpression[ST_ZMin](geometry)

  def ST_GeometricMedian(geometry: Column): Column =
    wrapExpression[ST_GeometricMedian](geometry, 1e-6, 1000, false)
  def ST_GeometricMedian(geometry: String): Column =
    wrapExpression[ST_GeometricMedian](geometry, 1e-6, 1000, false)
  def ST_GeometricMedian(geometry: Column, tolerance: Column): Column =
    wrapExpression[ST_GeometricMedian](geometry, tolerance, 1000, false)
  def ST_GeometricMedian(geometry: String, tolerance: Double): Column =
    wrapExpression[ST_GeometricMedian](geometry, tolerance, 1000, false)
  def ST_GeometricMedian(geometry: Column, tolerance: Column, maxIter: Column): Column =
    wrapExpression[ST_GeometricMedian](geometry, tolerance, maxIter, false)
  def ST_GeometricMedian(geometry: String, tolerance: Double, maxIter: Int): Column =
    wrapExpression[ST_GeometricMedian](geometry, tolerance, maxIter, false)
  def ST_GeometricMedian(
      geometry: Column,
      tolerance: Column,
      maxIter: Column,
      failIfNotConverged: Column): Column =
    wrapExpression[ST_GeometricMedian](geometry, tolerance, maxIter, failIfNotConverged)
  def ST_GeometricMedian(
      geometry: String,
      tolerance: Double,
      maxIter: Int,
      failIfNotConverged: Boolean): Column =
    wrapExpression[ST_GeometricMedian](geometry, tolerance, maxIter, failIfNotConverged)

  def ST_DistanceSphere(a: Column, b: Column): Column = wrapExpression[ST_DistanceSphere](a, b)
  def ST_DistanceSphere(a: String, b: String): Column = wrapExpression[ST_DistanceSphere](a, b)
  def ST_DistanceSphere(a: Column, b: Column, c: Column): Column =
    wrapExpression[ST_DistanceSphere](a, b, c)
  def ST_DistanceSphere(a: String, b: String, c: Double): Column =
    wrapExpression[ST_DistanceSphere](a, b, c)

  def ST_DistanceSpheroid(a: Column, b: Column): Column =
    wrapExpression[ST_DistanceSpheroid](a, b)

  def ST_DistanceSpheroid(a: String, b: String): Column =
    wrapExpression[ST_DistanceSpheroid](a, b)

  def ST_AreaSpheroid(a: Column): Column = wrapExpression[ST_AreaSpheroid](a)

  def ST_AreaSpheroid(a: String): Column = wrapExpression[ST_AreaSpheroid](a)

  def ST_LengthSpheroid(a: Column): Column = wrapExpression[ST_LengthSpheroid](a)

  def ST_LengthSpheroid(a: String): Column = wrapExpression[ST_LengthSpheroid](a)

  def ST_NumPoints(geometry: Column): Column = wrapExpression[ST_NumPoints](geometry)

  def ST_NumPoints(geometry: String): Column = wrapExpression[ST_NumPoints](geometry)

  def ST_Force3D(geometry: Column): Column = wrapExpression[ST_Force3D](geometry, 0.0)

  def ST_Force3D(geometry: String): Column = wrapExpression[ST_Force3D](geometry, 0.0)

  def ST_Force3D(geometry: Column, zValue: Column): Column =
    wrapExpression[ST_Force3D](geometry, zValue)

  def ST_Force3D(geometry: String, zValue: Double): Column =
    wrapExpression[ST_Force3D](geometry, zValue)

  def ST_Force3DM(geometry: Column): Column = wrapExpression[ST_Force3DM](geometry, 0.0)
  def ST_Force3DM(geometry: String): Column = wrapExpression[ST_Force3DM](geometry, 0.0)
  def ST_Force3DM(geometry: Column, zValue: Column): Column =
    wrapExpression[ST_Force3DM](geometry, zValue)
  def ST_Force3DM(geometry: String, zValue: Double): Column =
    wrapExpression[ST_Force3DM](geometry, zValue)

  def ST_Force3DZ(geometry: Column): Column = wrapExpression[ST_Force3DZ](geometry, 0.0)
  def ST_Force3DZ(geometry: String): Column = wrapExpression[ST_Force3DZ](geometry, 0.0)
  def ST_Force3DZ(geometry: Column, zValue: Column): Column =
    wrapExpression[ST_Force3DZ](geometry, zValue)
  def ST_Force3DZ(geometry: String, zValue: Double): Column =
    wrapExpression[ST_Force3DZ](geometry, zValue)

  def ST_Force4D(geometry: Column): Column = wrapExpression[ST_Force4D](geometry, 0.0, 0.0)
  def ST_Force4D(geometry: String): Column = wrapExpression[ST_Force4D](geometry, 0.0, 0.0)
  def ST_Force4D(geometry: Column, zValue: Column, mValue: Column): Column =
    wrapExpression[ST_Force4D](geometry, zValue, mValue)
  def ST_Force4D(geometry: String, zValue: Double, mValue: Double): Column =
    wrapExpression[ST_Force4D](geometry, zValue, mValue)

  def ST_ForceCollection(geometry: Column): Column = wrapExpression[ST_ForceCollection](geometry)

  def ST_ForceCollection(geometry: String): Column = wrapExpression[ST_ForceCollection](geometry)

  def ST_ForcePolygonCW(geometry: Column): Column = wrapExpression[ST_ForcePolygonCW](geometry)
  def ST_ForcePolygonCW(geometry: String): Column = wrapExpression[ST_ForcePolygonCW](geometry)

  def ST_ForceRHR(geometry: Column): Column = wrapExpression[ST_ForceRHR](geometry)
  def ST_ForceRHR(geometry: String): Column = wrapExpression[ST_ForceRHR](geometry)

  def ST_GeneratePoints(geometry: Column, numPoints: Column): Column =
    wrapExpression[ST_GeneratePoints](geometry, numPoints)
  def ST_GeneratePoints(geometry: String, numPoints: String): Column =
    wrapExpression[ST_GeneratePoints](geometry, numPoints)
  def ST_GeneratePoints(geometry: String, numPoints: Integer): Column =
    wrapExpression[ST_GeneratePoints](geometry, numPoints)
  def ST_GeneratePoints(geometry: String, numPoints: Integer, seed: Integer): Column =
    wrapExpression[ST_GeneratePoints](geometry, numPoints, seed)
  def ST_GeneratePoints(geometry: String, numPoints: String, seed: String): Column =
    wrapExpression[ST_GeneratePoints](geometry, numPoints, seed)
  def ST_GeneratePoints(geometry: Column, numPoints: Column, seed: Column): Column =
    wrapExpression[ST_GeneratePoints](geometry, numPoints, seed)

  def ST_NRings(geometry: Column): Column = wrapExpression[ST_NRings](geometry)

  def ST_NRings(geometry: String): Column = wrapExpression[ST_NRings](geometry)

  def ST_Translate(geometry: Column, deltaX: Column, deltaY: Column, deltaZ: Column): Column =
    wrapExpression[ST_Translate](geometry, deltaX, deltaY, deltaZ)

  def ST_Translate(geometry: String, deltaX: Double, deltaY: Double, deltaZ: Double): Column =
    wrapExpression[ST_Translate](geometry, deltaX, deltaY, deltaZ)

  def ST_Translate(geometry: Column, deltaX: Column, deltaY: Column): Column =
    wrapExpression[ST_Translate](geometry, deltaX, deltaY, 0.0)

  def ST_Translate(geometry: String, deltaX: Double, deltaY: Double): Column =
    wrapExpression[ST_Translate](geometry, deltaX, deltaY, 0.0)

  def ST_TriangulatePolygon(geom: Column): Column = wrapExpression[ST_TriangulatePolygon](geom)
  def ST_TriangulatePolygon(geom: String): Column = wrapExpression[ST_TriangulatePolygon](geom)

  def ST_VoronoiPolygons(geometry: Column, tolerance: Column, extendTo: Column): Column =
    wrapExpression[ST_VoronoiPolygons](geometry, tolerance, extendTo)

  def ST_VoronoiPolygons(geometry: String, tolerance: Double, extendTo: String): Column =
    wrapExpression[ST_VoronoiPolygons](geometry, tolerance, extendTo)

  def ST_VoronoiPolygons(geometry: Column, tolerance: Column): Column =
    wrapExpression[ST_VoronoiPolygons](geometry, tolerance, null)

  def ST_VoronoiPolygons(geometry: String, tolerance: Double): Column =
    wrapExpression[ST_VoronoiPolygons](geometry, tolerance, null)

  def ST_VoronoiPolygons(geometry: Column): Column =
    wrapExpression[ST_VoronoiPolygons](geometry, 0.0, null)

  def ST_VoronoiPolygons(geometry: String): Column =
    wrapExpression[ST_VoronoiPolygons](geometry, 0.0, null)

  def ST_FrechetDistance(g1: Column, g2: Column): Column =
    wrapExpression[ST_FrechetDistance](g1, g2)

  def ST_FrechetDistance(g1: String, g2: String): Column =
    wrapExpression[ST_FrechetDistance](g1, g2)

  def ST_Affine(
      geometry: Column,
      a: Column,
      b: Column,
      c: Column,
      d: Column,
      e: Column,
      f: Column,
      g: Column,
      h: Column,
      i: Column,
      xOff: Column,
      yOff: Column,
      zOff: Column): Column =
    wrapExpression[ST_Affine](geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff)

  def ST_Affine(
      geometry: String,
      a: Double,
      b: Double,
      c: Double,
      d: Double,
      e: Double,
      f: Double,
      g: Double,
      h: Double,
      i: Double,
      xOff: Double,
      yOff: Double,
      zOff: Double): Column =
    wrapExpression[ST_Affine](geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff)

  def ST_Affine(
      geometry: Column,
      a: Column,
      b: Column,
      d: Column,
      e: Column,
      xOff: Column,
      yOff: Column) =
    wrapExpression[ST_Affine](geometry, a, b, d, e, xOff, yOff)

  def ST_Affine(
      geometry: String,
      a: Double,
      b: Double,
      d: Double,
      e: Double,
      xOff: Double,
      yOff: Double) =
    wrapExpression[ST_Affine](geometry, a, b, d, e, xOff, yOff)

  def ST_BoundingDiagonal(geometry: Column) =
    wrapExpression[ST_BoundingDiagonal](geometry)

  def ST_BoundingDiagonal(geometry: String) =
    wrapExpression[ST_BoundingDiagonal](geometry)

  def ST_Angle(p1: Column, p2: Column, p3: Column, p4: Column): Column =
    wrapExpression[ST_Angle](p1, p2, p3, p4)

  def ST_Angle(p1: String, p2: String, p3: String, p4: String): Column =
    wrapExpression[ST_Angle](p1, p2, p3, p4)

  def ST_Angle(p1: Column, p2: Column, p3: Column): Column = wrapExpression[ST_Angle](p1, p2, p3)

  def ST_Angle(p1: String, p2: String, p3: String): Column = wrapExpression[ST_Angle](p1, p2, p3)

  def ST_Angle(line1: Column, line2: Column): Column = wrapExpression[ST_Angle](line1, line2)

  def ST_Angle(line1: String, line2: String): Column = wrapExpression[ST_Angle](line1, line2)

  def ST_Degrees(angleInRadian: Column): Column = wrapExpression[ST_Degrees](angleInRadian)

  def ST_Degrees(angleInRadian: Double): Column = wrapExpression[ST_Degrees](angleInRadian)

  def ST_DelaunayTriangles(geometry: Column, tolerance: Column, flags: Column): Column =
    wrapExpression[ST_DelaunayTriangles](geometry, tolerance, flags)
  def ST_DelaunayTriangles(geometry: String, tolerance: Double, flags: Integer): Column =
    wrapExpression[ST_DelaunayTriangles](geometry, tolerance, flags)
  def ST_DelaunayTriangles(geometry: Column, tolerance: Column): Column =
    wrapExpression[ST_DelaunayTriangles](geometry, tolerance)
  def ST_DelaunayTriangles(geometry: String, tolerance: Double): Column =
    wrapExpression[ST_DelaunayTriangles](geometry, tolerance)
  def ST_DelaunayTriangles(geometry: Column): Column =
    wrapExpression[ST_DelaunayTriangles](geometry)
  def ST_DelaunayTriangles(geometry: String): Column =
    wrapExpression[ST_DelaunayTriangles](geometry)

  def ST_HausdorffDistance(g1: Column, g2: Column) =
    wrapExpression[ST_HausdorffDistance](g1, g2, -1)

  def ST_HausdorffDistance(g1: String, g2: String) =
    wrapExpression[ST_HausdorffDistance](g1, g2, -1);

  def ST_HausdorffDistance(g1: Column, g2: Column, densityFrac: Column) =
    wrapExpression[ST_HausdorffDistance](g1, g2, densityFrac);

  def ST_HausdorffDistance(g1: String, g2: String, densityFrac: Double) =
    wrapExpression[ST_HausdorffDistance](g1, g2, densityFrac);

  def ST_CoordDim(geometry: Column): Column = wrapExpression[ST_CoordDim](geometry)

  def ST_CoordDim(geometry: String): Column = wrapExpression[ST_CoordDim](geometry)

  def ST_IsCollection(geometry: Column): Column = wrapExpression[ST_IsCollection](geometry)

  def ST_IsCollection(geometry: String): Column = wrapExpression[ST_IsCollection](geometry)

}
