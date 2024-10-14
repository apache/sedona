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

import org.apache.sedona.common.sphere.Haversine
import org.locationtech.jts.geom.{Envelope, Geometry}

/**
 * Utility functions for generating geometries for spatial join.
 */
object JoinedGeometry {

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
