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

import org.locationtech.jts.geom.{Envelope, Geometry}

/**
 * Utility functions for generating geometries for spatial join.
 */
object JoinedGeometry {
  /**
   * Convert the given geometry to an envelope expanded by distance.
   * @param geom the geometry to expand
   * @param distance the distance to expand
   * @param isGeography whether the geometry is on a sphere
   * @return the expanded envelope
   */
  def geometryToExpandedEnvelope(geom: Geometry, distance: Double, isGeography: Boolean): Geometry = {
    val envelope = geom.getEnvelopeInternal.copy()
    val expandedEnvelope = if (isGeography) {
      // Here we use the polar radius of the spheroid as the radius of the sphere, so that the expanded
      // envelope will work for both spherical and spheroidal distances.
      expandEnvelopeForGeography(envelope, distance, 6357000.0)
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
   * @param envelope     the envelope to expand
   * @param distance     in meter
   * @param sphereRadius in meter
   */
  def expandEnvelopeForGeography(envelope: Envelope, distance: Double, sphereRadius: Double): Envelope = {
    val scaleFactor = 1.1 // 10% buffer to get rid of false negatives
    val latDeltaRadian = distance / sphereRadius
    val latDeltaDegree = Math.toDegrees(latDeltaRadian)
    val newMinY = envelope.getMinY - latDeltaDegree * scaleFactor
    val newMaxY = envelope.getMaxY + latDeltaDegree * scaleFactor
    if (newMinY <= -90 || newMaxY >= 90) {
      new Envelope(-180, 180, Math.max(newMinY, -90), Math.min(newMaxY, 90))
    } else {
      val minLatRadian = Math.toRadians(newMinY)
      val maxLatRadian = Math.toRadians(newMaxY)
      val lonDeltaRadian = Math.max(Math.abs(distance / (sphereRadius * Math.cos(maxLatRadian))),
        Math.abs(distance / (sphereRadius * Math.cos(minLatRadian))))
      val lonDeltaDegree = Math.toDegrees(lonDeltaRadian)
      val newMinX = envelope.getMinX - lonDeltaDegree * scaleFactor
      val newMaxX = envelope.getMaxX + lonDeltaDegree * scaleFactor
      if (newMinX <= -180 || newMaxX >= 180) {
        new Envelope(-180, 180, newMinY, newMaxY)
      } else {
        new Envelope(newMinX, newMaxX, newMinY, newMaxY)
      }
    }
  }
}
