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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.Column
import org.apache.spark.sql.sedona_sql.expressions.collect.{ST_Collect, ST_CollectionExtract}

object st_functions extends DataFrameAPI {
  def ST_3DDistance(a: ColumnOrName, b: ColumnOrName): Column = wrapExpression[ST_3DDistance](a, b)

  def ST_AddPoint(lineString: ColumnOrName, point: ColumnOrName): Column = wrapExpression[ST_AddPoint](lineString, point)
  def ST_AddPoint(lineString: ColumnOrName, point: ColumnOrName, index: ColumnOrName): Column = wrapExpression[ST_AddPoint](lineString, point, index)
  def ST_AddPoint(lineString: ColumnOrName, point: ColumnOrName, index: Int): Column = wrapExpression[ST_AddPoint](lineString, point, index)

  def ST_Area(geometry: ColumnOrName): Column = wrapExpression[ST_Area](geometry)

  def ST_AsBinary(geometry: ColumnOrName): Column = wrapExpression[ST_AsBinary](geometry)

  def ST_AsEWKB(geometry: ColumnOrName): Column = wrapExpression[ST_AsEWKB](geometry)

  def ST_AsEWKT(geometry: ColumnOrName): Column = wrapExpression[ST_AsEWKT](geometry)

  def ST_AsGeoJSON(geometry: ColumnOrName): Column = wrapExpression[ST_AsGeoJSON](geometry)

  def ST_AsGML(geometry: ColumnOrName): Column = wrapExpression[ST_AsGML](geometry)

  def ST_AsKML(geometry: ColumnOrName): Column = wrapExpression[ST_AsKML](geometry)

  def ST_AsText(geometry: ColumnOrName): Column = wrapExpression[ST_AsText](geometry)

  def ST_Azimuth(pointA: ColumnOrName, pointB: ColumnOrName): Column = wrapExpression[ST_Azimuth](pointA, pointB)

  def ST_Boundary(geometry: ColumnOrName): Column = wrapExpression[ST_Boundary](geometry)

  def ST_Buffer(geometry: ColumnOrName, buffer: ColumnOrNameOrNumber): Column = wrapExpression[ST_Buffer](geometry, buffer)

  def ST_BuildArea(geometry: ColumnOrName): Column = wrapExpression[ST_BuildArea](geometry)

  def ST_Centroid(geometry: ColumnOrName): Column = wrapExpression[ST_Centroid](geometry)

  def ST_Collect(geoms: ColumnOrName): Column = wrapExpression[ST_Collect](geoms)
  def ST_Collect(geoms: ColumnOrName*): Column = wrapVarArgExpression[ST_Collect](geoms)

  def ST_CollectionExtract(collection: ColumnOrName): Column = wrapExpression[ST_CollectionExtract](collection)
  def ST_CollectionExtract(collection: ColumnOrName, geomType: ColumnOrName): Column = wrapExpression[ST_CollectionExtract](collection, geomType)
  def ST_CollectionExtract(collection: ColumnOrName, geomType: Int): Column = wrapExpression[ST_CollectionExtract](collection, geomType)

  def ST_ConvexHull(geometry: ColumnOrName): Column = wrapExpression[ST_ConvexHull](geometry)
  
  def ST_Difference(a: ColumnOrName, b: ColumnOrName): Column = wrapExpression[ST_Difference](a, b)

  def ST_Distance(a: ColumnOrName, b: ColumnOrName): Column = wrapExpression[ST_Distance](a, b)

  def ST_Dump(geometry: ColumnOrName): Column = wrapExpression[ST_Dump](geometry)

  def ST_DumpPoints(geometry: ColumnOrName): Column = wrapExpression[ST_DumpPoints](geometry)

  def ST_EndPoint(lineString: ColumnOrName): Column = wrapExpression[ST_EndPoint](lineString)

  def ST_Envelope(geometry: ColumnOrName): Column = wrapExpression[ST_Envelope](geometry)

  def ST_ExteriorRing(polygon: ColumnOrName): Column = wrapExpression[ST_ExteriorRing](polygon)

  def ST_FlipCoordinates(geometry: ColumnOrName): Column = wrapExpression[ST_FlipCoordinates](geometry)
  
  def ST_Force_2D(geometry: ColumnOrName): Column = wrapExpression[ST_Force_2D](geometry)

  def ST_GeoHash(geometry: ColumnOrName, precision: ColumnOrName): Column = wrapExpression[ST_GeoHash](geometry, precision)
  def ST_GeoHash(geometry: ColumnOrName, precision: Int): Column = wrapExpression[ST_GeoHash](geometry, precision)

  def ST_GeometryN(multiGeometry: ColumnOrName, n: ColumnOrName): Column = wrapExpression[ST_GeometryN](multiGeometry, n)
  def ST_GeometryN(multiGeometry: ColumnOrName, n: Int): Column = wrapExpression[ST_GeometryN](multiGeometry, n)

  def ST_GeometryType(geometry: ColumnOrName): Column = wrapExpression[ST_GeometryType](geometry)

  def ST_InteriorRingN(polygon: ColumnOrName, n: ColumnOrName): Column = wrapExpression[ST_InteriorRingN](polygon, n)
  def ST_InteriorRingN(polygon: ColumnOrName, n: Int): Column = wrapExpression[ST_InteriorRingN](polygon, n)

  def ST_Intersection(a: ColumnOrName, b: ColumnOrName): Column = wrapExpression[ST_Intersection](a, b)

  def ST_IsClosed(geometry: ColumnOrName): Column = wrapExpression[ST_IsClosed](geometry)
  
  def ST_IsEmpty(geometry: ColumnOrName): Column = wrapExpression[ST_IsEmpty](geometry)

  def ST_IsRing(lineString: ColumnOrName): Column = wrapExpression[ST_IsRing](lineString)

  def ST_IsSimple(geometry: ColumnOrName): Column = wrapExpression[ST_IsSimple](geometry)

  def ST_IsValid(geometry: ColumnOrName): Column = wrapExpression[ST_IsValid](geometry)

  def ST_Length(geometry: ColumnOrName): Column = wrapExpression[ST_Length](geometry)

  def ST_LineInterpolatePoint(geometry: ColumnOrName, fraction: ColumnOrNameOrNumber): Column = wrapExpression[ST_LineInterpolatePoint](geometry, fraction)

  def ST_LineMerge(multiLineString: ColumnOrName): Column = wrapExpression[ST_LineMerge](multiLineString)

  def ST_LineSubstring(lineString: ColumnOrName, startFraction: ColumnOrNameOrNumber, endFraction: ColumnOrNameOrNumber): Column = wrapExpression[ST_LineSubstring](lineString, startFraction, endFraction)

  def ST_MakePolygon(lineString: ColumnOrName, holes: ColumnOrName): Column = wrapExpression[ST_MakePolygon](lineString, holes)

  def ST_MakeValid(geometry: ColumnOrName): Column = wrapExpression[ST_MakeValid](geometry)
  def ST_MakeValid(geometry: ColumnOrName, keepCollapsed: ColumnOrName): Column = wrapExpression[ST_MakeValid](geometry, keepCollapsed)
  def ST_MakeValid(geometry: ColumnOrName, keepCollapsed: Boolean): Column = wrapExpression[ST_MakeValid](geometry, keepCollapsed)

  def ST_MinimumBoundingCircle(geometry: ColumnOrName): Column = wrapExpression[ST_MinimumBoundingCircle](geometry)
  def ST_MinimumBoundingCircle(geometry: ColumnOrName, quadrantSegments: ColumnOrName): Column = wrapExpression[ST_MinimumBoundingCircle](geometry)
  def ST_MinimumBoundingCircle(geometry: ColumnOrName, quadrantSegments: Int): Column = wrapExpression[ST_MinimumBoundingCircle](geometry)

  def ST_MinimumBoundingRadius(geometry: ColumnOrName): Column = wrapExpression[ST_MinimumBoundingRadius](geometry)
  
  def ST_Multi(geometry: ColumnOrName): Column = wrapExpression[ST_Multi](geometry)

  def ST_Normalize(geometry: ColumnOrName): Column = wrapExpression[ST_Normalize](geometry)

  def ST_NPoints(geometry: ColumnOrName): Column = wrapExpression[ST_NPoints](geometry)

  def ST_NumGeometries(geometry: ColumnOrName): Column = wrapExpression[ST_NumGeometries](geometry)

  def ST_NumInteriorRings(geometry: ColumnOrName): Column = wrapExpression[ST_NumInteriorRings](geometry)
  
  def ST_PointN(geometry: ColumnOrName, n: ColumnOrName): Column = wrapExpression[ST_PointN](geometry, n)
  def ST_PointN(geometry: ColumnOrName, n: Int): Column = wrapExpression[ST_PointN](geometry, n)
  
  def ST_PointOnSurface(geometry: ColumnOrName): Column = wrapExpression[ST_PointOnSurface](geometry)

  def ST_PrecisionReduce(geometry: ColumnOrName, precision: ColumnOrName): Column = wrapExpression[ST_PrecisionReduce](geometry, precision)
  def ST_PrecisionReduce(geometry: ColumnOrName, precision: Int): Column = wrapExpression[ST_PrecisionReduce](geometry, precision)

  def ST_RemovePoint(lineString: ColumnOrName, index: ColumnOrName): Column = wrapExpression[ST_RemovePoint](lineString, index)
  def ST_RemovePoint(lineString: ColumnOrName, index: Int): Column = wrapExpression[ST_RemovePoint](lineString, index)
  
  def ST_Reverse(geometry: ColumnOrName): Column = wrapExpression[ST_Reverse](geometry)

  def ST_SetSRID(geometry: ColumnOrName, srid: Int): Column = wrapExpression[ST_SetSRID](geometry, srid)
  def ST_SetSRID(geometry: ColumnOrName, srid: ColumnOrName): Column = wrapExpression[ST_SetSRID](geometry, srid)

  def ST_SRID(geometry: ColumnOrName): Column = wrapExpression[ST_SRID](geometry)

  def ST_StartPoint(lineString: ColumnOrName): Column = wrapExpression[ST_StartPoint](lineString)

  def ST_SubDivide(geometry: ColumnOrName, maxVertices: ColumnOrName): Column = wrapExpression[ST_SubDivide](geometry, maxVertices)
  def ST_SubDivide(geometry: ColumnOrName, maxVertices: Int): Column = wrapExpression[ST_SubDivide](geometry, maxVertices)

  def ST_SubDivideExplode(geometry: ColumnOrName, maxVertices: ColumnOrName): Column = wrapExpression[ST_SubDivideExplode](geometry, maxVertices)
  def ST_SubDivideExplode(geometry: ColumnOrName, maxVertices: Int): Column = wrapExpression[ST_SubDivideExplode](geometry, maxVertices)

  def ST_SimplifyPreserveTopology(geometry: ColumnOrName, distanceTolerance: ColumnOrNameOrNumber): Column = wrapExpression[ST_SimplifyPreserveTopology](geometry, distanceTolerance)
  
  def ST_SymDifference(a: ColumnOrName, b: ColumnOrName): Column = wrapExpression[ST_SymDifference](a, b)

  def ST_Transform(geometry: ColumnOrName, sourceCRS: ColumnOrName, targetCRS: ColumnOrName): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS)
  def ST_Transform(geometry: ColumnOrName, sourceCRS: ColumnOrName, targetCRS: ColumnOrName, disableError: ColumnOrName): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, disableError)
  def ST_Transform(geometry: ColumnOrName, sourceCRS: ColumnOrName, targetCRS: ColumnOrName, disableError: Boolean): Column = wrapExpression[ST_Transform](geometry, sourceCRS, targetCRS, disableError)
  
  def ST_Union(a: ColumnOrName, b: ColumnOrName): Column = wrapExpression[ST_Union](a, b)
  
  def ST_X(point: ColumnOrName): Column = wrapExpression[ST_X](point)

  def ST_XMax(geometry: ColumnOrName): Column = wrapExpression[ST_XMax](geometry)
  
  def ST_XMin(geometry: ColumnOrName): Column = wrapExpression[ST_XMin](geometry)

  def ST_Y(point: ColumnOrName): Column = wrapExpression[ST_Y](point)

  def ST_YMax(geometry: ColumnOrName): Column = wrapExpression[ST_YMax](geometry)

  def ST_YMin(geometry: ColumnOrName): Column = wrapExpression[ST_YMin](geometry)

  def ST_Z(point: ColumnOrName): Column = wrapExpression[ST_Z](point)
}