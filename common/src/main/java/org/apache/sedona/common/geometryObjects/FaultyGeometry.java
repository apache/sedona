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
package org.apache.sedona.common.geometryObjects;

import org.locationtech.jts.geom.*;

public class FaultyGeometry extends Geometry {
  private final Geometry geometry;
  private final String errorMessage;

  public FaultyGeometry(Geometry geometry, String errorMessage) {
    super(geometry.getFactory());
    this.geometry = geometry;
    this.errorMessage = errorMessage;
  }

  public Geometry getGeometry() {
    return geometry;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String getGeometryType() {
    return geometry.getGeometryType();
  }

  @Override
  public Coordinate getCoordinate() {
    return geometry.getCoordinate();
  }

  @Override
  public Coordinate[] getCoordinates() {
    return geometry.getCoordinates();
  }

  @Override
  public int getNumPoints() {
    return geometry.getNumPoints();
  }

  @Override
  public boolean isEmpty() {
    return geometry.isEmpty();
  }

  @Override
  public int getDimension() {
    return geometry.getDimension();
  }

  @Override
  public Geometry getBoundary() {
    return geometry.getBoundary();
  }

  @Override
  public int getBoundaryDimension() {
    return geometry.getBoundaryDimension();
  }

  @Override
  protected Geometry reverseInternal() {
    return geometry.reverse();
  }

  @Override
  public boolean equalsExact(Geometry other, double tolerance) {
    return geometry.equalsExact(other, tolerance);
  }

  @Override
  public void apply(CoordinateFilter filter) {
    geometry.apply(filter);
  }

  @Override
  public void apply(CoordinateSequenceFilter filter) {
    geometry.apply(filter);
  }

  @Override
  public void apply(GeometryFilter filter) {
    geometry.apply(filter);
  }

  @Override
  public void apply(GeometryComponentFilter filter) {
    geometry.apply(filter);
  }

  @Override
  protected Geometry copyInternal() {
    return geometry.copy();
  }

  @Override
  public void normalize() {
    geometry.normalize();
  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    return geometry.getEnvelopeInternal();
  }

  @Override
  protected int compareToSameClass(Object o) {
    return geometry.compareTo(o);
  }

  @Override
  protected int compareToSameClass(Object o, CoordinateSequenceComparator comp) {
    return geometry.compareTo(o, comp);
  }

  @Override
  protected int getTypeCode() {
    return 0;
  }

  @Override
  public String toString() {
    return geometry.toString() + (errorMessage != null ? " - " + errorMessage : "");
  }
}
