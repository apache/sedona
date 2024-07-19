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
package org.apache.sedona.common;

import java.util.Set;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

public class FunctionsGeoTools {

  public static Geometry transform(Geometry geometry, String targetCRS)
      throws FactoryException, TransformException {
    return transform(geometry, null, targetCRS, true);
  }

  public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
      throws FactoryException, TransformException {
    return transform(geometry, sourceCRS, targetCRS, true);
  }

  public static Geometry transform(
      Geometry geometry, String sourceCRScode, String targetCRScode, boolean lenient)
      throws FactoryException, TransformException {
    CoordinateReferenceSystem targetCRS = parseCRSString(targetCRScode);
    return transformToGivenTarget(geometry, sourceCRScode, targetCRS, lenient);
  }

  /**
   * Transform a geometry from one CRS to another. If sourceCRS is not specified, it will be
   * extracted from the geometry. If lenient is true, the transformation will be lenient. This
   * function is used by the implicit CRS transformation in Sedona rasters.
   *
   * @param geometry
   * @param sourceCRScode
   * @param targetCRS
   * @param lenient
   * @return
   * @throws FactoryException
   * @throws TransformException
   */
  public static Geometry transformToGivenTarget(
      Geometry geometry, String sourceCRScode, CoordinateReferenceSystem targetCRS, boolean lenient)
      throws FactoryException, TransformException {
    // If sourceCRS is not specified, try to get it from the geometry
    if (sourceCRScode == null) {
      int srid = geometry.getSRID();
      if (srid != 0) {
        sourceCRScode = "epsg:" + srid;
      } else {
        // If SRID is not set, throw an exception
        throw new IllegalArgumentException(
            "Source CRS must be specified. No SRID found on geometry.");
      }
    }
    CoordinateReferenceSystem sourceCRS = parseCRSString(sourceCRScode);
    int targetSRID = crsToSRID(targetCRS);
    // If sourceCRS and targetCRS are equal, return the geometry unchanged
    if (!CRS.equalsIgnoreMetadata(sourceCRS, targetCRS)) {
      MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
      Geometry transformed = JTS.transform(geometry, transform);
      transformed = Functions.setSRID(transformed, targetSRID);
      transformed.setUserData(geometry.getUserData());
      return transformed;
    } else {
      if (geometry.getSRID() != targetSRID) {
        Geometry transformed = Functions.setSRID(geometry, targetSRID);
        transformed.setUserData(geometry.getUserData());
        return transformed;
      } else {
        return geometry;
      }
    }
  }

  /**
   * Get the SRID of a CRS. We use the EPSG code of the CRS if available.
   *
   * @param crs CoordinateReferenceSystem
   * @return SRID
   */
  public static int crsToSRID(CoordinateReferenceSystem crs) {
    Set<ReferenceIdentifier> crsIds = crs.getIdentifiers();
    for (ReferenceIdentifier crsId : crsIds) {
      if ("EPSG".equals(crsId.getCodeSpace())) {
        return Integer.parseInt(crsId.getCode());
      }
    }
    return 0;
  }

  /**
   * Decode SRID to CRS, forcing axis order to be lon/lat
   *
   * @param srid SRID
   * @return CoordinateReferenceSystem object
   */
  public static CoordinateReferenceSystem sridToCRS(int srid) {
    try {
      return CRS.decode("EPSG:" + srid, true);
    } catch (FactoryException e) {
      throw new IllegalArgumentException("Cannot decode SRID " + srid, e);
    }
  }

  private static CoordinateReferenceSystem parseCRSString(String CRSString)
      throws FactoryException {
    try {
      // Try to parse as a well-known CRS code
      // Longitude first, then latitude
      return CRS.decode(CRSString, true);
    } catch (NoSuchAuthorityCodeException e) {
      try {
        // Try to parse as a WKT CRS string, longitude first
        Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        return ReferencingFactoryFinder.getCRSFactory(hints).createFromWKT(CRSString);
      } catch (FactoryException ex) {
        throw new FactoryException(
            "First failed to read as a well-known CRS code: \n"
                + e.getMessage()
                + "\nThen failed to read as a WKT CRS string: \n"
                + ex.getMessage());
      }
    }
  }

  public static Geometry voronoiPolygons(Geometry geom, double tolerance, Geometry extendTo) {
    if (geom == null) {
      return null;
    }
    VoronoiDiagramBuilder builder = new VoronoiDiagramBuilder();
    builder.setSites(geom);
    builder.setTolerance(tolerance);
    if (extendTo != null) {
      builder.setClipEnvelope(extendTo.getEnvelopeInternal());
    } else {
      Envelope e = geom.getEnvelopeInternal();
      e.expandBy(Math.max(e.getWidth(), e.getHeight()));
      builder.setClipEnvelope(e);
    }
    return builder.getDiagram(geom.getFactory());
  }

  public static Geometry bufferSpheroid(Geometry geometry, double radius, BufferParameters params)
      throws IllegalArgumentException {
    // Determine the best SRID for spheroidal calculations
    int bestCRS = Functions.bestSRID(geometry);
    int originalCRS = geometry.getSRID();
    final int WGS84CRS = 4326;

    // Shift longitude if geometry crosses dateline
    if (Functions.crossesDateLine(geometry)) {
      Functions.shiftLongitude(geometry);
    }
    // geometry = (Predicates.crossesDateLine(geometry)) ? Functions.shiftLongitude(geometry)
    // : geometry;

    // If originalCRS is not set, use WGS84 as the originalCRS for transformation
    String sourceCRSCode = (originalCRS == 0) ? "EPSG:" + WGS84CRS : "EPSG:" + originalCRS;
    String targetCRSCode = "EPSG:" + bestCRS;

    try {
      // Transform the geometry to the selected SRID
      Geometry transformedGeometry = transform(geometry, sourceCRSCode, targetCRSCode);
      // Apply the buffer operation in the selected SRID
      Geometry bufferedGeometry = BufferOp.bufferOp(transformedGeometry, radius, params);

      // Transform back to the original SRID or to WGS 84 if original SRID was not set
      int backTransformCRSCode = (originalCRS == 0) ? WGS84CRS : originalCRS;
      Geometry bufferedResult =
          transform(bufferedGeometry, targetCRSCode, "EPSG:" + backTransformCRSCode);

      // Normalize longitudes between -180 and 180
      Functions.normalizeLongitude(bufferedResult);
      return bufferedResult;
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException(e);
    }
  }
}
