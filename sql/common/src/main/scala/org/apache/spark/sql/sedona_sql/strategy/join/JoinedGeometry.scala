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
    // Here we use the polar radius of the spheroid as the radius of the sphere, so that the expanded
    // envelope will work for both spherical and spheroidal distances.
    expandEnvelope(envelope, distance, 6357000.0, isGeography)
    geom.getFactory.toGeometry(envelope)
  }

  /**
   * Expand the given envelope by the given distance in meter.
   * For geography, we expand the envelope by the given distance in both longitude and latitude.
   *
   * @param envelope the envelope to expand
   * @param distance in meter
   * @param sphereRadius in meter
   * @param isGeography whether the envelope is on a sphere
   */
  private def expandEnvelope(envelope: Envelope, distance: Double, sphereRadius: Double, isGeography: Boolean): Unit = {
    if (isGeography) {
      val scaleFactor = 1.1 // 10% buffer to get rid of false negatives
      val latRadian = Math.toRadians((envelope.getMinY + envelope.getMaxY) / 2.0)
      val latDeltaRadian = distance / sphereRadius;
      val latDeltaDegree = Math.toDegrees(latDeltaRadian)
      val lonDeltaRadian = Math.max(Math.abs(distance / (sphereRadius * Math.cos(latRadian + latDeltaRadian))),
        Math.abs(distance / (sphereRadius * Math.cos(latRadian - latDeltaRadian))))
      val lonDeltaDegree = Math.toDegrees(lonDeltaRadian)
      envelope.expandBy(latDeltaDegree * scaleFactor, lonDeltaDegree * scaleFactor)
    } else {
      envelope.expandBy(distance)
    }
  }
}
