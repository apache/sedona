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

import static org.apache.sis.referencing.IdentifiedObjects.lookupEPSG;

import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.crs.AbstractCRS;
import org.apache.sis.referencing.cs.AxesConvention;
import org.apache.sis.util.Classes;
import org.locationtech.jts.geom.*;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.util.FactoryException;

public class FunctionsApacheSIS {

  private static final String EPSG = "EPSG";

  /**
   * Transforms the given geometry to the specified Coordinate Reference System (CRS). If the given
   * CRS or the given geometry is null or is the same as current CRS, then the geometry is returned
   * unchanged. If the geometry has no Coordinate Reference System, an exception is thrown.
   *
   * @param geometry the geometry to transform, or {@code null}.
   * @param targetCRS the target coordinate reference system, or {@code null}.
   * @return the transformed geometry, or the same geometry if it is already in target CRS.
   */
  public static Geometry transform(Geometry geometry, String targetCRS) {
    return transformToGivenTarget(geometry, null, targetCRS, true);
  }

  /**
   * Transforms the given geometry to the specified Coordinate Reference System (CRS). If the given
   * CRS or the given geometry is null or is the same as current CRS, then the geometry is returned
   * unchanged. If the geometry has no Coordinate Reference System, an exception is thrown.
   *
   * @param geometry the geometry to transform, or {@code null}.
   * @param sourceCRS the source coordinate reference system, or {@code null}.
   * @param targetCRS the target coordinate reference system, or {@code null}.
   * @return the transformed geometry, or the same geometry if it is already in target CRS.
   */
  public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS) {
    return transformToGivenTarget(geometry, sourceCRS, targetCRS, true);
  }

  /**
   * Transforms the given geometry to the specified Coordinate Reference System (CRS). If the given
   *
   * @param geometry the geometry to transform, or {@code null}.
   * @param sourceCRScode the source coordinate reference system code, or {@code null}.
   * @param targetCRScode the target coordinate reference system code, or {@code null}.
   * @param lenient whether to be lenient in case of failure to find the operation.
   * @return the transformed geometry, or the same geometry if it is already in target CRS.
   */
  public static Geometry transform(
      Geometry geometry, String sourceCRScode, String targetCRScode, boolean lenient) {
    return transformToGivenTarget(geometry, sourceCRScode, targetCRScode, lenient);
  }

  public static Geometry transformToGivenTarget(
      Geometry geometry, String sourceCRScode, String targetCRScode, boolean lenient) {
    if (sourceCRScode == null && geometry.getSRID() == 0) {
      throw new IllegalArgumentException(
          "Source CRS must be specified. No SRID found on geometry.");
    }

    // If sourceCRS is not specified, try to get it from the geometry
    if (sourceCRScode == null) {
      int srid = geometry.getSRID();
      sourceCRScode = EPSG + ":" + srid;
    }

    CoordinateReferenceSystem sourceCRS, targetCRS;
    try {
      sourceCRS = parseCRSString(sourceCRScode);
      targetCRS = parseCRSString(targetCRScode);
    } catch (FactoryException e) {
      throw new RuntimeException(
          String.format("Failed to parse CRS from code %s or %s", sourceCRScode, targetCRScode), e);
    }
    return transformToGivenTarget(geometry, sourceCRS, targetCRS, lenient);
  }

  /**
   * Transforms the given geometry to the specified Coordinate Reference System (CRS). If the given
   * CRS or the given geometry is null or is the same as current CRS, then the geometry is returned
   * unchanged. If the geometry has no Coordinate Reference System, then the geometry is returned
   * unchanged.
   *
   * @param geometry the geometry to transform, or {@code null}.
   * @param targetCRS the target coordinate reference system, or {@code null}.
   * @return the transformed geometry, or the same geometry if it is already in target CRS.
   */
  public static Geometry transformToGivenTarget(
      Geometry geometry,
      CoordinateReferenceSystem sourceCRS,
      CoordinateReferenceSystem targetCRS,
      boolean lenient)
      throws RuntimeException {
    if (geometry == null || targetCRS == null) {
      // nothing to do, return the geometry unchanged.
      return geometry;
    }

    MathTransform transform;
    try {
      /* Force coordinates to Longitude/Latitude (X,Y) order for transformation. Doing this during
       * CRS parsing loses information about the CRS (including the EPSG code), but it is necessary
       * for the transformation.
       *
       * <p>Note: The area of interest is not currently used in the function. There are cases where
       * it can improve accuracy.
       */
      CoordinateOperation operation =
          CRS.findOperation(asLonLat(sourceCRS), asLonLat(targetCRS), null);

      transform = operation.getMathTransform();
    } catch (FactoryException e) {
      throw new RuntimeException(
          String.format(
              "Failed to find operation for transformation from %s to %s",
              sourceCRS.getName(), targetCRS.getName()),
          e);
    }

    if (transform.isIdentity()) {
      try {
        int srid = lookupEPSG(targetCRS);
        return Functions.setSRID(geometry, srid);
      } catch (FactoryException e) {
        throw new RuntimeException(
            "Failed to find EPSG code during transform for CRS" + targetCRS.getName(), e);
      }
    }

    Geometry targetGeometry;
    try {

      final var gct = new GeometryCoordinateTransform(transform, geometry.getFactory());
      targetGeometry = gct.transform(geometry);
      targetGeometry = Functions.setSRID(targetGeometry, lookupEPSG(targetCRS));
      targetGeometry.setUserData(geometry.getUserData());

    } catch (TransformException e) {
      if (lenient) {
        // If lenient, return the original geometry
        return geometry;
      }
      throw new RuntimeException(
          String.format(
              "Failed transform from %s to %s for geometry %s",
              sourceCRS.getName(), targetCRS.getName(), geometry),
          e);
    } catch (FactoryException e) {
      throw new RuntimeException(
          "Failed to find EPSG code during transform for CRS" + targetCRS, e);
    }
    return targetGeometry;
  }

  public static CoordinateReferenceSystem parseCRSString(String CRSString) throws FactoryException {
    CoordinateReferenceSystem crs;
    try {
      crs = CRS.forCode(CRSString);
    } catch (NoSuchAuthorityCodeException e) {
      try {
        crs = CRS.fromWKT(CRSString);
      } catch (FactoryException ex) {
        throw new IllegalArgumentException(
            "First failed to read as a well-known CRS code: \n"
                + e.getMessage()
                + "\nThen failed to read as a WKT CRS string: \n"
                + ex.getMessage());
      }
    }
    // Do not force lon/lat ordering here, as the CRS information (including EPSG code) may be lost.
    return crs;
  }

  private static CoordinateReferenceSystem asLonLat(CoordinateReferenceSystem crs) {
    if (crs == null) {
      return null;
    }
    return AbstractCRS.castOrCopy(crs).forConvention(AxesConvention.RIGHT_HANDED);
  }

  /**
   * This is replicated from the Apache SIS project, as it's not available in the public API.
   *
   * <p>An operation transforming a geometry into another geometry. This class decomposes the
   * geometry into it's most primitive elements, the {@link CoordinateSequence}, applies an
   * operation, then rebuilds the geometry. The operation may change coordinate values (for example
   * a map projection), but not necessarily. An operation could also be a clipping for example.
   */
  public static class GeometryCoordinateTransform {
    /** The factory to use for creating geometries. */
    private final GeometryFactory geometryFactory;

    /** The factory to use for creating sequences of coordinate tuples. */
    protected final CoordinateSequenceFactory coordinateFactory;

    /** The transform to apply on coordinate values. */
    private final MathTransform transform;

    /**
     * A temporary buffer holding coordinates to transform. Created when first needed in order to
     * have an estimation of size needed.
     */
    private double[] coordinates;

    /**
     * Creates a new geometry transformer using the given coordinate transform. It is caller's
     * responsibility to ensure that the number of source and target dimensions of the given
     * transform are equal to the number of dimensions of the geometries to transform.
     *
     * @param transform the transform to apply on coordinate values.
     * @param factory the factory to use for creating geometries. Shall not be null.
     */
    public GeometryCoordinateTransform(
        final MathTransform transform, final GeometryFactory factory) {
      this.geometryFactory = factory;
      this.coordinateFactory = factory.getCoordinateSequenceFactory();
      this.transform = transform;
    }

    public CoordinateSequence transform(final CoordinateSequence sequence, final int minPoints)
        throws TransformException {
      final int srcDim = transform.getSourceDimensions();
      final int tgtDim = transform.getTargetDimensions();
      final int maxDim = Math.max(srcDim, tgtDim);
      final int count = sequence.size();
      final int capacity = Math.max(4, Math.min(100, count));
      final CoordinateSequence out = coordinateFactory.create(count, tgtDim);
      if (coordinates == null || coordinates.length / maxDim < capacity) {
        coordinates = new double[capacity * maxDim];
      }
      for (int base = 0, n; (n = Math.min(count - base, capacity)) > 0; base += n) {
        int batch = n * srcDim;
        for (int i = 0; i < batch; i++) {
          coordinates[i] = sequence.getOrdinate(base + i / srcDim, i % srcDim);
        }
        transform.transform(coordinates, 0, coordinates, 0, n);
        batch = n * tgtDim;
        for (int i = 0; i < batch; i++) {
          out.setOrdinate(base + i / tgtDim, i % tgtDim, coordinates[i]);
        }
      }
      return out;
    }

    /**
     * Transforms the given geometry. This method delegates to one of the {@code transform(…)}
     * methods based on the type of the given geometry.
     *
     * @param geom the geometry to transform.
     * @return the transformed geometry.
     * @throws TransformException if an error occurred while transforming the geometry.
     */
    public Geometry transform(final Geometry geom) throws TransformException {
      if (geom instanceof Point) return transform((Point) geom);
      if (geom instanceof MultiPoint) return transform((MultiPoint) geom);
      if (geom instanceof LinearRing)
        return transform((LinearRing) geom); // Must be tested before LineString.
      if (geom instanceof LineString) return transform((LineString) geom);
      if (geom instanceof MultiLineString) return transform((MultiLineString) geom);
      if (geom instanceof Polygon) return transform((Polygon) geom);
      if (geom instanceof MultiPolygon) return transform((MultiPolygon) geom);
      if (geom instanceof GeometryCollection) return transform((GeometryCollection) geom);
      throw new IllegalArgumentException(
          "Unsupported geometry type: " + Classes.getShortClassName(geom));
    }

    /**
     * Transforms the given point. Can be invoked directly if the type is known at compile-time, or
     * indirectly through a call to the more generic {@link #transform(Geometry)} method.
     *
     * @param geom the point to transform.
     * @return the transformed point.
     * @throws TransformException if an error occurred while transforming the geometry.
     */
    public Point transform(final Point geom) throws TransformException {
      final CoordinateSequence coord = geom.getCoordinateSequence();
      return geometryFactory.createPoint(transform(coord, 1));
    }

    /**
     * Transforms the given points. Can be invoked directly if the type is known at compile-time, or
     * indirectly through a call to the more generic {@link #transform(Geometry)} method.
     *
     * @param geom the points to transform.
     * @return the transformed points.
     * @throws TransformException if an error occurred while transforming a geometry.
     */
    public MultiPoint transform(final MultiPoint geom) throws TransformException {
      final var subs = new Point[geom.getNumGeometries()];
      for (int i = 0; i < subs.length; i++) {
        subs[i] = transform((Point) geom.getGeometryN(i));
      }
      return geometryFactory.createMultiPoint(subs);
    }

    /**
     * Transforms the given line string. Can be invoked directly if the type is known at
     * compile-time, or indirectly through a call to the more generic {@link #transform(Geometry)}
     * method.
     *
     * @param geom the line string to transform.
     * @return the transformed line string.
     * @throws TransformException if an error occurred while transforming the geometry.
     */
    public LineString transform(final LineString geom) throws TransformException {
      final CoordinateSequence seq = transform(geom.getCoordinateSequence(), 2);
      return geometryFactory.createLineString(seq);
    }

    /**
     * Transforms the given line strings. Can be invoked directly if the type is known at
     * compile-time, or indirectly through a call to the more generic {@link #transform(Geometry)}
     * method.
     *
     * @param geom the line strings to transform.
     * @return the transformed line strings.
     * @throws TransformException if an error occurred while transforming a geometry.
     */
    public MultiLineString transform(final MultiLineString geom) throws TransformException {
      final var subs = new LineString[geom.getNumGeometries()];
      for (int i = 0; i < subs.length; i++) {
        subs[i] = transform((LineString) geom.getGeometryN(i));
      }
      return geometryFactory.createMultiLineString(subs);
    }

    /**
     * Transforms the given linear ring. Can be invoked directly if the type is known at
     * compile-time, or indirectly through a call to the more generic {@link #transform(Geometry)}
     * method.
     *
     * @param geom the linear ring to transform.
     * @return the transformed linear ring.
     * @throws TransformException if an error occurred while transforming the geometry.
     */
    public LinearRing transform(final LinearRing geom) throws TransformException {
      final CoordinateSequence seq = transform(geom.getCoordinateSequence(), 4);
      return geometryFactory.createLinearRing(seq);
    }

    /**
     * Transforms the given polygon. Can be invoked directly if the type is known at compile-time,
     * or indirectly through a call to the more generic {@link #transform(Geometry)} method.
     *
     * @param geom the polygon to transform.
     * @return the transformed polygon.
     * @throws TransformException if an error occurred while transforming the geometry.
     */
    public Polygon transform(final Polygon geom) throws TransformException {
      final LinearRing exterior = transform(geom.getExteriorRing());
      final var holes = new LinearRing[geom.getNumInteriorRing()];
      for (int i = 0; i < holes.length; i++) {
        holes[i] = transform(geom.getInteriorRingN(i));
      }
      return geometryFactory.createPolygon(exterior, holes);
    }

    /**
     * Transforms the given polygons. Can be invoked directly if the type is known at compile-time,
     * or indirectly through a call to the more generic {@link #transform(Geometry)} method.
     *
     * @param geom the polygons to transform.
     * @return the transformed polygons.
     * @throws TransformException if an error occurred while transforming a geometry.
     */
    public MultiPolygon transform(final MultiPolygon geom) throws TransformException {
      final var subs = new Polygon[geom.getNumGeometries()];
      for (int i = 0; i < subs.length; i++) {
        subs[i] = transform((Polygon) geom.getGeometryN(i));
      }
      return geometryFactory.createMultiPolygon(subs);
    }

    /**
     * Transforms the given geometries. Can be invoked directly if the type is known at
     * compile-time, or indirectly through a call to the more generic {@link #transform(Geometry)}
     * method.
     *
     * @param geom the geometries to transform.
     * @return the transformed geometries.
     * @throws TransformException if an error occurred while transforming a geometry.
     */
    public GeometryCollection transform(final GeometryCollection geom) throws TransformException {
      final var subs = new Geometry[geom.getNumGeometries()];
      for (int i = 0; i < subs.length; i++) {
        subs[i] = transform(geom.getGeometryN(i));
      }
      return geometryFactory.createGeometryCollection(subs);
    }
  }
}
