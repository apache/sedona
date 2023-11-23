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
package org.apache.sedona.common.raster;

import org.apache.sedona.common.Functions;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

public class FunctionsTest extends RasterTestBase {

    @Test
    public void testSetSrid() throws FactoryException {
        assertEquals(0, RasterAccessors.srid(oneBandRaster));
        assertEquals(4326, RasterAccessors.srid(multiBandRaster));

        GridCoverage2D oneBandRasterWithUpdatedSrid = RasterEditors.setSrid(oneBandRaster, 4326);
        assertEquals(4326, RasterAccessors.srid(oneBandRasterWithUpdatedSrid));
        assertEquals(4326, GeometryFunctions.envelope(oneBandRasterWithUpdatedSrid).getSRID());
        assertTrue(GeometryFunctions.envelope(oneBandRasterWithUpdatedSrid).equalsTopo(GeometryFunctions.envelope(oneBandRaster)));

        AffineTransform2D oneBandAffine = RasterUtils.getGDALAffineTransform(oneBandRaster);
        AffineTransform2D oneBandUpdatedAffine = RasterUtils.getGDALAffineTransform(oneBandRasterWithUpdatedSrid);
        Assert.assertEquals(oneBandAffine, oneBandUpdatedAffine);

        GridCoverage2D multiBandRasterWithUpdatedSrid = RasterEditors.setSrid(multiBandRaster, 0);
        assertEquals(0 , RasterAccessors.srid(multiBandRasterWithUpdatedSrid));

        AffineTransform2D multiBandAffine = RasterUtils.getGDALAffineTransform(multiBandRaster);
        AffineTransform2D multiBandUpdatedAffine = RasterUtils.getGDALAffineTransform(multiBandRasterWithUpdatedSrid);
        Assert.assertEquals(multiBandAffine, multiBandUpdatedAffine);
    }

    @Test
    public void value() throws TransformException {
        assertNull("Points outside of the envelope should return null.", PixelFunctions.value(oneBandRaster, point(1, 1), 1));

        Double value = PixelFunctions.value(oneBandRaster, point(378923, 4072346), 1);
        assertNotNull(value);
        assertEquals(2.0d, value, 0.1d);

        assertNull("Null should be returned for no data values.", PixelFunctions.value(oneBandRaster, point(378923, 4072376), 1));
    }

    @Test
    public void valueWithGridCoords() throws TransformException {
        int insideX = 1;
        int insideY = 0;
        int outsideX = 4;
        int outsideY = 4;

        Double insideValue = PixelFunctions.value(oneBandRaster, insideX, insideY, 1);
        assertNotNull("Value should not be null for points inside the envelope.", insideValue);
        assertNull("Points outside of the envelope should return null.", PixelFunctions.value(oneBandRaster, outsideX, outsideY, 1));

        int noDataX = 0;
        int noDataY = 0;

        assertNull("Null should be returned for no data values.", PixelFunctions.value(oneBandRaster, noDataX, noDataY, 1));
    }

    @Test
    public void valueWithMultibandRaster() throws TransformException {
        // Multiband raster
        assertEquals(9d, PixelFunctions.value(multiBandRaster, point(4.5d,4.5d), 3), 0.1d);
        assertEquals(255d, PixelFunctions.value(multiBandRaster, point(4.5d,4.5d), 4), 0.1d);
        assertEquals(4d, PixelFunctions.value(multiBandRaster, 2,2, 3), 0.1d);
        assertEquals(255d, PixelFunctions.value(multiBandRaster, 3,4, 4), 0.1d);
    }

    @Test
    public void testPixelAsPolygon() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 123, -230, 8);
        String actual = Functions.asWKT(PixelFunctions.getPixelAsPolygon(emptyRaster, 2, 3));
        String expected = "POLYGON ((131 -246, 139 -246, 139 -254, 131 -254, 131 -246))";
        assertEquals(expected, actual);

        // Testing with skewed rasters
        emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 234, -43, 3, 4, 2,3,0);
        actual = Functions.asWKT(PixelFunctions.getPixelAsPolygon(emptyRaster, 2, 3));
        expected = "POLYGON ((241 -32, 244 -29, 246 -25, 243 -28, 241 -32))";
        assertEquals(expected,actual);
    }

    @Test
    public void testPixelAsPointsPolygons() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 123, -230, 8);
        List<PixelRecord> points = PixelFunctions.getPixelAsPolygons(emptyRaster, 1);

        PixelRecord point = points.get(11);
        Geometry geom = (Geometry) point.geom;
        String expected = "POLYGON ((131 -246, 139 -246, 139 -254, 131 -254, 131 -246))";
        assertEquals(expected,geom.toString());

        // Testing with skewed raster
        emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 234, -43, 3, 4, 2,3,0);
        points = PixelFunctions.getPixelAsPolygons(emptyRaster, 1);
        point = points.get(11);
        geom = (Geometry) point.geom;
        expected = "POLYGON ((241 -32, 244 -29, 246 -25, 243 -28, 241 -32))";
        assertEquals(expected,geom.toString());
    }

    @Test
    public void testPixelAsCentroid() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 12, 13, 134, -53, 9);
        String actual = Functions.asWKT(PixelFunctions.getPixelAsCentroid(emptyRaster, 3, 3));
        String expected = "POINT (156.5 -75.5)";
        assertEquals(expected, actual);

        // Testing with skewed raster
        emptyRaster = RasterConstructors.makeEmptyRaster(1, 12, 13, 240, -193, 2, 1.5, 3, 2, 0);
        actual = Functions.asWKT(PixelFunctions.getPixelAsCentroid(emptyRaster, 3, 3));
        expected = "POINT (252.5 -184.25)";
        assertEquals(expected, actual);
    }

    @Test
    public void testPixelAsCentroids() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 12, 13, 134, -53, 9);
        List<PixelRecord> points = PixelFunctions.getPixelAsCentroids(emptyRaster, 1);
        String expected = "POINT (156.5 -75.5)";
        PixelRecord point = points.get(26);
        Geometry geom = point.geom;
        assertEquals(expected, geom.toString());

        // Testing with skewed raster
        emptyRaster = RasterConstructors.makeEmptyRaster(1, 12, 13, 240, -193, 2, 1.5, 3, 2, 0);
        points = PixelFunctions.getPixelAsCentroids(emptyRaster, 1);
        expected = "POINT (252.5 -184.25)";
        point = points.get(26);
        geom = point.geom;
        assertEquals(expected, geom.toString());
    }

    @Test
    public void testPixelAsPointUpperLeft() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 123, -230, 8);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 1, 1);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(123, coordinates.x, 1e-9);
        assertEquals(-230, coordinates.y, 1e-9);
        assertEquals(0, actualPoint.getSRID());
    }

    @Test
    public void testPixelAsPointMiddle() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 10, 123, -230, 8);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 3, 5);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(139, coordinates.x, 1e-9);
        assertEquals(-262, coordinates.y, 1e-9);
        assertEquals(0, actualPoint.getSRID());
    }

    @Test
    public void testPixelAsPointCustomSRIDPlanar() throws FactoryException, TransformException {
        int srid = 3857;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, -123, 54, 5, 5, 0, 0, srid);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 1, 1);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(-123, coordinates.x, 1e-9);
        assertEquals(54, coordinates.y, 1e-9);
        assertEquals(srid, actualPoint.getSRID());
    }

    @Test
    public void testPixelAsPointSRIDSpherical() throws FactoryException, TransformException {
        int srid = 4326;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, srid);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 2, 3);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(-118, coordinates.x, 1e-9);
        assertEquals(34, coordinates.y, 1e-9);
        assertEquals(srid, actualPoint.getSRID());
    }

    @Test
    public void testPixelAsPointOutOfBounds() throws FactoryException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 10, 123, -230, 8);
        Exception e = assertThrows(IndexOutOfBoundsException.class, () -> PixelFunctions.getPixelAsPoint(emptyRaster, 6, 1));
        String expectedMessage = "Specified pixel coordinates (6, 1) do not lie in the raster";
        assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    public void testPixelAsPointFromRasterFile() throws IOException, TransformException, FactoryException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(raster, 1, 1);
        Coordinate coordinate = actualPoint.getCoordinate();
        double expectedX = -13095817.809482181;
        double expectedY = 4021262.7487925636;
        assertEquals(expectedX, coordinate.getX(), 0.2d);
        assertEquals(expectedY, coordinate.getY(), 0.2d);
    }

    @Test
    public void testPixelAsPointSkewedRaster() throws FactoryException, TransformException {
        // Testing with skewed raster
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 12, 13, 240, -193, 2, 1.5, 3, 2, 0);
        String actual = Functions.asWKT(PixelFunctions.getPixelAsPoint(emptyRaster, 3, 3));
        String expected = "POINT (250 -186)";
        assertEquals(expected, actual);
    }

    @Test
    public void testPixelAsPointsOutputSize() throws FactoryException, TransformException {
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, 5, 10, 123, -230, 8);
        List<PixelRecord> points = PixelFunctions.getPixelAsPoints(raster, 1);
        assertEquals(50, points.size());
    }

    @Test
    public void testPixelAsPointsValues() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 123, -230, 8);
        List<PixelRecord> points = PixelFunctions.getPixelAsPoints(emptyRaster, 1);

        PixelRecord point1 = points.get(0);
        Geometry geom1 = (Geometry) point1.geom;
        assertEquals(123, geom1.getCoordinate().x, FP_TOLERANCE);
        assertEquals(-230, geom1.getCoordinate().y, FP_TOLERANCE);
        assertEquals(0.0, point1.value, FP_TOLERANCE);

        PixelRecord point2 = points.get(22);
        Geometry geom2 = (Geometry) point2.geom;
        assertEquals(139, geom2.getCoordinate().x, FP_TOLERANCE);
        assertEquals(-262, geom2.getCoordinate().y, FP_TOLERANCE);
        assertEquals(0.0, point2.value, FP_TOLERANCE);
    }

    @Test
    public void testPixelAsPointsCustomSRIDPlanar() throws FactoryException, TransformException {
        int srid = 3857;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 5, -123, 54, 5, 5, 0, 0, srid);
        List<PixelRecord> points = PixelFunctions.getPixelAsPoints(emptyRaster, 1);
        PixelRecord point1 = points.get(0);
        Geometry geom1 = (Geometry) point1.geom;
        assertEquals(-123, geom1.getCoordinate().x, FP_TOLERANCE);
        assertEquals(54, geom1.getCoordinate().y, FP_TOLERANCE);
        assertEquals(srid, geom1.getSRID());
    }

    @Test
    public void testPixelAsPointsSRIDSpherical() throws FactoryException, TransformException {
        int srid = 4326;
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, -123, 54, 5, -10, 0, 0, srid);
        List<PixelRecord> points = PixelFunctions.getPixelAsPoints(emptyRaster, 1);

        PixelRecord point1 = points.get(11);
        Geometry geom1 = (Geometry) point1.geom;
        assertEquals(-118, geom1.getCoordinate().x, FP_TOLERANCE);
        assertEquals(34, geom1.getCoordinate().y, FP_TOLERANCE);
        assertEquals(srid, geom1.getSRID());
    }

    @Test
    public void testPixelAsPointsFromRasterFile() throws IOException, TransformException, FactoryException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/test1.tiff");
        List<PixelRecord> points = PixelFunctions.getPixelAsPoints(raster, 1);
        PixelRecord firstPoint = points.get(0);
        Geometry firstGeom = (Geometry) firstPoint.geom;

        double expectedX = -1.3095818E7;
        double expectedY = 4021262.75;
        double val = 0.0;

        assertEquals(expectedX, firstGeom.getCoordinate().x, FP_TOLERANCE);
        assertEquals(expectedY, firstGeom.getCoordinate().y, FP_TOLERANCE);
        assertEquals(val, firstPoint.value, FP_TOLERANCE);
    }

    @Test
    public void testPixelAsPointsSkewedRaster() throws FactoryException, TransformException {
        // Testing with skewed raster
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 12, 13, 240, -193, 2, 1.5, 3, 2, 0);
        List<PixelRecord> points = PixelFunctions.getPixelAsPoints(emptyRaster, 1);

        PixelRecord point1 = points.get(26);
        Geometry geom1 = (Geometry) point1.geom;
        String expected = "POINT (250 -186)";
        assertEquals(expected, geom1.toString());
    }

    @Test
    public void values() throws TransformException {
        // The function 'value' is implemented using 'values'.
        // These test only cover bits not already covered by tests for 'value'
        List<Geometry> points = Arrays.asList(new Geometry[]{point(378923, 4072346), point(378924, 4072346)});
        List<Double> values = PixelFunctions.values(oneBandRaster, points, 1);
        assertEquals(2, values.size());
        assertTrue(values.stream().allMatch(Objects::nonNull));

        values = PixelFunctions.values(oneBandRaster, Arrays.asList(new Geometry[]{point(378923, 4072346), null}), 1);
        assertEquals(2, values.size());
        assertNull("Null geometries should return null values.", values.get(1));
    }

    @Test
    public void valuesWithGridCoords() throws TransformException {
        int[] xCoordinates = {1, 0};
        int[] yCoordinates = {0, 1};

        List<Double> values = PixelFunctions.values(oneBandRaster, xCoordinates, yCoordinates, 1);
        assertEquals(2, values.size());
        assertTrue(values.stream().allMatch(Objects::nonNull));
    }

    private Point point(double x, double y) {
        return new GeometryFactory().createPoint(new Coordinate(x, y));
    }
}
