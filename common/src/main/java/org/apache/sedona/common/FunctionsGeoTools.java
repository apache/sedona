/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.triangulate.VoronoiDiagramBuilder;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;




public class FunctionsGeoTools {
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    public static Geometry transform(Geometry geometry, String targetCRS)
            throws FactoryException, TransformException {
        return transform(geometry, null, targetCRS, true);
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
        throws FactoryException, TransformException {
        return transform(geometry, sourceCRS, targetCRS, true);
    }

    public static Geometry transform(Geometry geometry, String sourceCRScode, String targetCRScode, boolean lenient)
        throws FactoryException, TransformException {
        CoordinateReferenceSystem targetCRS = parseCRSString(targetCRScode);
        return transformToGivenTarget(geometry, sourceCRScode, targetCRS, lenient);
    }

    /**
     * Transform a geometry from one CRS to another. If sourceCRS is not specified, it will be
     * extracted from the geometry. If lenient is true, the transformation will be lenient.
     * This function is used by the implicit CRS transformation in Sedona rasters.
     * @param geometry
     * @param sourceCRScode
     * @param targetCRS
     * @param lenient
     * @return
     * @throws FactoryException
     * @throws TransformException
     */
    public static Geometry transformToGivenTarget(Geometry geometry, String sourceCRScode, CoordinateReferenceSystem targetCRS, boolean lenient)
            throws FactoryException, TransformException
    {
        // If sourceCRS is not specified, try to get it from the geometry
        if (sourceCRScode == null) {
            int srid = geometry.getSRID();
            if (srid != 0) {
                sourceCRScode = "epsg:" + srid;
            }
            else {
                // If SRID is not set, throw an exception
                throw new IllegalArgumentException("Source CRS must be specified. No SRID found on geometry.");
            }
        }
        CoordinateReferenceSystem sourceCRS = parseCRSString(sourceCRScode);
        // If sourceCRS and targetCRS are equal, return the geometry unchanged
        if (!CRS.equalsIgnoreMetadata(sourceCRS, targetCRS)) {
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
            return JTS.transform(geometry, transform);
        }
        else return geometry;
    }

    /**
     * Decode SRID to CRS, forcing axis order to be lon/lat
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
            throws FactoryException
    {
        try {
            // Try to parse as a well-known CRS code
            // Longitude first, then latitude
            return CRS.decode(CRSString, true);
        }
        catch (NoSuchAuthorityCodeException e) {
            try {
                // Try to parse as a WKT CRS string, longitude first
                Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
                return ReferencingFactoryFinder.getCRSFactory(hints).createFromWKT(CRSString);
            }
            catch (FactoryException ex) {
                throw new FactoryException("First failed to read as a well-known CRS code: \n" + e.getMessage() + "\nThen failed to read as a WKT CRS string: \n" + ex.getMessage());
            }
        }
    }

    public static Geometry voronoiPolygons(Geometry geom, double tolerance, Geometry extendTo) {
        if(geom == null) {
            return null;
        }
        VoronoiDiagramBuilder builder = new VoronoiDiagramBuilder();
        builder.setSites(geom);
        builder.setTolerance(tolerance);
        if (extendTo != null) {
            builder.setClipEnvelope(extendTo.getEnvelopeInternal());
        }
        else{
            Envelope e = geom.getEnvelopeInternal();
            e.expandBy(Math.max(e.getWidth(), e.getHeight()));
            builder.setClipEnvelope(e);
        }
        return builder.getDiagram(FunctionsGeoTools.GEOMETRY_FACTORY);
    }
}
