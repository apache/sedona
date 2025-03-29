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
package org.apache.sedona.core.wrapper;

import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.locationtech.jts.geom.*;

public class UniqueGeometry<T> extends Geometry {
  private final T originalGeometry;
  private final String uniqueId;

  public UniqueGeometry(T originalGeometry) {
    super(new GeometryFactory());
    this.originalGeometry = originalGeometry;
    this.uniqueId = UUID.randomUUID().toString();
  }

  public T getOriginalGeometry() {
    return originalGeometry;
  }

  public String getUniqueId() {
    return uniqueId;
  }

  @Override
  public int hashCode() {
    return uniqueId.hashCode(); // Uniqueness ensured by uniqueId
  }

  @Override
  public String getGeometryType() {
    throw new NotImplementedException("getGeometryType is not implemented.");
  }

  @Override
  public Coordinate getCoordinate() {
    throw new NotImplementedException("getCoordinate is not implemented.");
  }

  @Override
  public Coordinate[] getCoordinates() {
    throw new NotImplementedException("getCoordinates is not implemented.");
  }

  @Override
  public int getNumPoints() {
    throw new NotImplementedException("getNumPoints is not implemented.");
  }

  @Override
  public boolean isEmpty() {
    throw new NotImplementedException("isEmpty is not implemented.");
  }

  @Override
  public int getDimension() {
    throw new NotImplementedException("getDimension is not implemented.");
  }

  @Override
  public Geometry getBoundary() {
    throw new NotImplementedException("getBoundary is not implemented.");
  }

  @Override
  public int getBoundaryDimension() {
    throw new NotImplementedException("getBoundaryDimension is not implemented.");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    UniqueGeometry<?> that = (UniqueGeometry<?>) obj;
    return uniqueId.equals(that.uniqueId);
  }

  @Override
  public String toString() {
    return "UniqueGeometry{"
        + "originalGeometry="
        + originalGeometry
        + ", uniqueId='"
        + uniqueId
        + '\''
        + '}';
  }

  @Override
  protected Geometry reverseInternal() {
    throw new NotImplementedException("reverseInternal is not implemented.");
  }

  @Override
  public boolean equalsExact(Geometry geometry, double v) {
    throw new NotImplementedException("equalsExact is not implemented.");
  }

  @Override
  public void apply(CoordinateFilter coordinateFilter) {
    throw new NotImplementedException("apply(CoordinateFilter) is not implemented.");
  }

  @Override
  public void apply(CoordinateSequenceFilter coordinateSequenceFilter) {
    throw new NotImplementedException("apply(CoordinateSequenceFilter) is not implemented.");
  }

  @Override
  public void apply(GeometryFilter geometryFilter) {
    throw new NotImplementedException("apply(GeometryFilter) is not implemented.");
  }

  @Override
  public void apply(GeometryComponentFilter geometryComponentFilter) {
    throw new NotImplementedException("apply(GeometryComponentFilter) is not implemented.");
  }

  @Override
  protected Geometry copyInternal() {
    throw new NotImplementedException("copyInternal is not implemented.");
  }

  @Override
  public void normalize() {
    throw new NotImplementedException("normalize is not implemented.");
  }

  @Override
  protected Envelope computeEnvelopeInternal() {
    throw new NotImplementedException("computeEnvelopeInternal is not implemented.");
  }

  @Override
  protected int compareToSameClass(Object o) {
    throw new NotImplementedException("compareToSameClass(Object) is not implemented.");
  }

  @Override
  protected int compareToSameClass(
      Object o, CoordinateSequenceComparator coordinateSequenceComparator) {
    throw new NotImplementedException(
        "compareToSameClass(Object, CoordinateSequenceComparator) is not implemented.");
  }

  @Override
  protected int getTypeCode() {
    throw new NotImplementedException("getTypeCode is not implemented.");
  }
}
