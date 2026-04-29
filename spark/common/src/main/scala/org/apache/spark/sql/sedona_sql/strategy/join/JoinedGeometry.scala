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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.S2Geography.Geography
import org.apache.sedona.common.sphere.Haversine
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.locationtech.jts.geom.{Envelope, Geometry, GeometryFactory}

/**
 * Payload stored in `userData` on each Geography index entry. Carries both the deserialized
 * Geography (for S2 predicate refinement) and the original row (for the join output).
 */
case class GeographyJoinShape(geog: Geography, row: UnsafeRow)

/**
 * Utility functions for generating geometries for spatial join.
 */
object JoinedGeometry {

  private val DEFAULT_FACTORY = new GeometryFactory()

  /**
   * Convert a Geography to a JTS Geometry whose envelope covers the Geography's lat/lng bounding
   * rectangle. When the rectangle wraps the antimeridian we expand to the full longitude range
   * [-180, 180]; this is a coarse filter that keeps the planar index simple (apache/sedona-db PR
   * #775 made the same trade-off; #782 tracks the eventual split-at-±180 optimisation).
   */
  def geographyToEnvelopeGeometry(geog: Geography): Geometry = {
    val rect = geog.region().getRectBound
    val latLo = rect.latLo().degrees()
    val latHi = rect.latHi().degrees()
    val (lngLo, lngHi) = if (rect.lng().isInverted) {
      (-180.0, 180.0)
    } else {
      (rect.lngLo().degrees(), rect.lngHi().degrees())
    }
    DEFAULT_FACTORY.toGeometry(new Envelope(lngLo, lngHi, latLo, latHi))
  }

  /**
   * Convert the given geometry to an envelope expanded by distance.
   * @param geom
   *   the geometry to expand
   * @param distance
   *   the distance to expand
   * @param isGeography
   *   whether the geometry is on a sphere
   * @return
   *   the expanded envelope
   */
  def geometryToExpandedEnvelope(
      geom: Geometry,
      distance: Double,
      isGeography: Boolean): Geometry = {
    val envelope = geom.getEnvelopeInternal.copy()
    val expandedEnvelope = if (isGeography) {
      expandEnvelopeForGeography(envelope, distance)
    } else {
      val newEnvelope = envelope.copy()
      newEnvelope.expandBy(distance)
      newEnvelope
    }
    geom.getFactory.toGeometry(expandedEnvelope)
  }

  /**
   * Expand the given envelope by the given distance in meter.
   *
   * @param envelope
   *   the envelope to expand
   * @param distance
   *   in meter
   */
  private def expandEnvelopeForGeography(envelope: Envelope, distance: Double): Envelope = {
    // Here we use the polar radius of the spheroid as the radius of the sphere, so that the expanded
    // envelope will work for both spherical and spheroidal distances.
    val sphereRadius = 6357000.0
    Haversine.expandEnvelope(envelope, distance, sphereRadius)
  }
}
