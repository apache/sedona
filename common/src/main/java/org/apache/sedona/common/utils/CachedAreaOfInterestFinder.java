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
package org.apache.sedona.common.utils;

import static org.apache.sedona.common.FunctionsApacheSIS.asLonLat;
import static org.apache.sedona.common.FunctionsApacheSIS.parseCRSString;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Objects;
import org.apache.sis.geometry.Envelopes;
import org.apache.sis.geometry.GeneralEnvelope;
import org.apache.sis.metadata.iso.extent.DefaultGeographicBoundingBox;
import org.apache.sis.referencing.CommonCRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.geometry.Envelope;
import org.opengis.metadata.extent.GeographicBoundingBox;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.FactoryException;

/**
 * A utility class for finding Area of Interest transforms. It avoids the expensive equals() call on
 * CRS objects in the CRS.findTransform method.
 */
public class CachedAreaOfInterestFinder {
  private CachedAreaOfInterestFinder() {}

  // EPSG:4326 with coordinate order (longitude, latitude) and range [-180° to +180°]
  static final CoordinateReferenceSystem WGS84 = asLonLat(CommonCRS.WGS84.geographic());
  static final String WGS84_CODE = "EPSG:4326";

  private static class BBoxPair {
    private final String sourceCRS;
    private final Envelope bounds;
    private final int hashCode;

    public BBoxPair(Envelope bounds, final String sourceCRS) {
      this.sourceCRS = sourceCRS;
      this.bounds = bounds;
      this.hashCode = Objects.hash(sourceCRS, bounds);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof BBoxPair)) {
        return false;
      }
      BBoxPair other = (BBoxPair) obj;
      return this.bounds.getMinimum(0) == other.bounds.getMinimum(0)
          && this.bounds.getMaximum(0) == other.bounds.getMaximum(0)
          && this.bounds.getMinimum(1) == other.bounds.getMinimum(1)
          && this.bounds.getMaximum(1) == other.bounds.getMaximum(1)
          && this.sourceCRS.equals(other.sourceCRS); // fix me, pull from the envelope
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }

  private static final LoadingCache<BBoxPair, GeographicBoundingBox> aoiCache =
      Caffeine.newBuilder().maximumSize(100).build(CachedAreaOfInterestFinder::doFindAOI);

  public static GeographicBoundingBox findAOI(Geometry aoi, String transformSourceCRS) {
    if (aoi == null) {
      return null;
    }
    org.locationtech.jts.geom.Envelope jtsEnvelope = aoi.getEnvelopeInternal();
    CoordinateReferenceSystem crs;

    String sourceCRS;
    if (aoi.getSRID() != 0) {
      sourceCRS = "EPSG:" + aoi.getSRID();
    } else if (transformSourceCRS != null && !transformSourceCRS.isEmpty()) {
      sourceCRS = transformSourceCRS;
    } else {
      sourceCRS = WGS84_CODE;
    }

    try {
      crs = asLonLat(parseCRSString(sourceCRS));
    } catch (FactoryException e) {
      throw new RuntimeException("Failed to find CRS for code: " + sourceCRS, e);
    }

    GeneralEnvelope bounds = new GeneralEnvelope(crs);
    bounds.setRange(0, jtsEnvelope.getMinX(), jtsEnvelope.getMaxX());
    bounds.setRange(1, jtsEnvelope.getMinY(), jtsEnvelope.getMaxY());

    return aoiCache.get(new BBoxPair(bounds, sourceCRS));
  }

  private static GeographicBoundingBox doFindAOI(BBoxPair bboxPair) {
    Envelope bounds = bboxPair.bounds;
    try {
      var transformed = Envelopes.transform(bounds, WGS84);
      double[] lowerCorner = transformed.getLowerCorner().getCoordinate();
      double[] upperCorner = transformed.getUpperCorner().getCoordinate();
      return new DefaultGeographicBoundingBox(
          lowerCorner[0], upperCorner[0], // Longitudes
          lowerCorner[1], upperCorner[1] // Latitudes.
          );
    } catch (TransformException ex) {
      throw new RuntimeException(
          "Failed to transform bounds from " + bboxPair.sourceCRS + " to WGS84");
    }
  }
}
