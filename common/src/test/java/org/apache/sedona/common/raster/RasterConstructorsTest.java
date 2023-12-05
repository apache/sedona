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

import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.io.netcdf.NetCDFReader;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import ucar.nc2.NetcdfFile;
//import ucar.nc2.NetcdfFiles;
//import ucar.nc2.NetcdfFiles;

import java.awt.image.Raster;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.*;

public class RasterConstructorsTest
        extends RasterTestBase {

    @Test
    public void fromArcInfoAsciiGrid() throws IOException, FactoryException {
        GridCoverage2D gridCoverage2D = RasterConstructors.fromArcInfoAsciiGrid(arc.getBytes(StandardCharsets.UTF_8));

        Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(3600, envelope.getArea(), 0.1);
        assertEquals(378922d + 30, envelope.getCentroid().getX(), 0.1);
        assertEquals(4072345d + 30, envelope.getCentroid().getY(), 0.1);
        assertEquals(2, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(2, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(0d, RasterUtils.getNoDataValue(gridCoverage2D.getSampleDimension(0)), 0.1);
        assertEquals(3d, gridCoverage2D.getRenderedImage().getData().getPixel(1, 1, (double[])null)[0], 0.1);
    }

    @Test
    public void fromGeoTiff() throws IOException, FactoryException {
        GridCoverage2D gridCoverage2D = RasterConstructors.fromGeoTiff(geoTiff);

        Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(100, envelope.getArea(), 0.1);
        assertEquals(5, envelope.getCentroid().getX(), 0.1);
        assertEquals(5, envelope.getCentroid().getY(), 0.1);
        assertEquals(10, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(10, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(10d, gridCoverage2D.getRenderedImage().getData().getPixel(5, 5, (double[])null)[0], 0.1);
        assertEquals(4, gridCoverage2D.getNumSampleDimensions());
    }

    @Test
    public void testAsRasterWithEmptyRaster() throws FactoryException, ParseException {
        // Polygon
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -1, 2, -2, 0, 0, 4326);
        Geometry geom = Constructors.geomFromWKT("POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))", 0);
        GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
        double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
        double[] expected = new double[] {3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d");
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {1.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        // MultiPolygon
        geom = Constructors.geomFromWKT("MULTIPOLYGON (((15 15, 1.5 5.5, 3.5 5.5, 3.5 1.5, 15 15)), ((4.4 2.4, 4.4 6.4, 6.4 6.4, 6.4 2.4, 4.4 2.4)))", 0);
        rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d");
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        // MultiLineString
        geom = Constructors.geomFromWKT("MULTILINESTRING ((5 5, 10 10), (10 10, 15 15, 20 20))", 0);
        rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d");
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        // LinearRing
        geom = Constructors.geomFromWKT("LINEARRING (10 10, 18 20, 15 24, 24 25, 10 10)", 0);
        rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d");
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        // MultiPoints
        geom = Constructors.geomFromWKT("MULTIPOINT ((5 5), (10 10), (15 15))", 0);
        rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3093151.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d");
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        // Point
        geom = Constructors.geomFromWKT("POINT (5 5)", 0);
        rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 3d);
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {3093151.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d");
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {1.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testAsRasterLingString() throws FactoryException, ParseException {
        // Horizontal LineString
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, 255, 255, 1, -1, 2, -2, 0, 0, 4326);
        Geometry geom = Constructors.geomFromEWKT("LINESTRING(1 1, 2 1, 10 1)");
        GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 0d);
        double[] actual = MapAlgebra.bandAsArray(rasterized, 1);
        double[] expected = new double[] {3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        // Vertical LineString
        geom = Constructors.geomFromEWKT("LINESTRING(1 1, 1 2, 1 10)");
        rasterized = RasterConstructors.asRaster(geom, raster, "d", 3093151, 0d);
        actual = MapAlgebra.bandAsArray(rasterized, 1);
        expected = new double[] {0.0, 0.0, 0.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0, 3093151.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testAsRasterWithRaster() throws IOException, ParseException, FactoryException {
        //Polygon
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
        Geometry geom = Constructors.geomFromWKT("POLYGON((1.5 1.5, 3.8 3.0, 4.5 4.4, 3.4 3.5, 1.5 1.5))", 0);
        GridCoverage2D rasterized = RasterConstructors.asRaster(geom, raster, "d", 612028, 5d);
        double[] actual = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).toArray();
        double[] expected = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 612028.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        rasterized = RasterConstructors.asRaster(geom, raster, "d", 5484);
        actual = Arrays.stream(MapAlgebra.bandAsArray(rasterized, 1)).toArray();
        expected = new double[] {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5484.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testAsRasterWithRasterExtent() throws IOException, ParseException, FactoryException {
        GridCoverage2D raster = rasterFromGeoTiff(resourceFolder + "raster/raster_with_no_data/test5.tiff");
        Geometry geom = Constructors.geomFromWKT("POLYGON((1.5 1.5, 3.8 3.0, 4.5 4.4, 3.4 3.5, 1.5 1.5))", RasterAccessors.srid(raster));
        GridCoverage2D rasterized = RasterConstructors.asRasterWithRasterExtent(geom, raster, "d", 612028, 5d);

        int widthActual = RasterAccessors.getWidth(rasterized);
        int widthExpected = RasterAccessors.getWidth(raster);
        assertEquals(widthExpected, widthActual);

        int heightActual = RasterAccessors.getHeight(rasterized);
        int heightExpected = RasterAccessors.getHeight(raster);
        assertEquals(heightExpected, heightActual);
    }

    @Test
    public void makeEmptyRaster() throws FactoryException {
        double upperLeftX = 0;
        double upperLeftY = 0;
        int widthInPixel = 1;
        int heightInPixel = 2;
        double pixelSize = 2;
        int numBands = 1;
        String dataType = "I";

        GridCoverage2D gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
        Geometry envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * pixelSize, envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("REAL_64BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize);
        envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * pixelSize, envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("SIGNED_32BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

        assertEquals("POLYGON ((0 -4, 0 0, 2 0, 2 -4, 0 -4))", envelope.toString());
        double expectedWidthInDegree = pixelSize * widthInPixel;
        double expectedHeightInDegree = pixelSize * heightInPixel;

        assertEquals(expectedWidthInDegree * expectedHeightInDegree, envelope.getArea(), 0.001);
        assertEquals(heightInPixel, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(widthInPixel, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(0d, gridCoverage2D.getRenderedImage().getData().getPixel(0, 0, (double[])null)[0], 0.001);
        assertEquals(1, gridCoverage2D.getNumSampleDimensions());

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize - 1, 0, 0, 0);
        envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * (pixelSize + 1), envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("REAL_64BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());

        gridCoverage2D = RasterConstructors.makeEmptyRaster(numBands, dataType, widthInPixel, heightInPixel, upperLeftX, upperLeftY, pixelSize, -pixelSize - 1, 0, 0, 0);
        envelope = GeometryFunctions.envelope(gridCoverage2D);
        assertEquals(upperLeftX, envelope.getEnvelopeInternal().getMinX(), 0.001);
        assertEquals(upperLeftX + widthInPixel * pixelSize, envelope.getEnvelopeInternal().getMaxX(), 0.001);
        assertEquals(upperLeftY - heightInPixel * (pixelSize + 1), envelope.getEnvelopeInternal().getMinY(), 0.001);
        assertEquals(upperLeftY, envelope.getEnvelopeInternal().getMaxY(), 0.001);
        assertEquals("SIGNED_32BITS", gridCoverage2D.getSampleDimension(0).getSampleDimensionType().name());
    }

    @Test
    public void testNetCdfClassic() throws FactoryException, IOException, TransformException {
        GridCoverage2D testRaster = RasterConstructors.fromNetCDF(testNc, "O3");
        double[] expectedMetadata = {4.9375, 50.9375, 80, 48, 0.125, -0.125, 0, 0, 0, 4};
        double[] actualMetadata = RasterAccessors.metadata(testRaster);
        for (int i = 0; i < expectedMetadata.length; i++) {
            assertEquals(expectedMetadata[i], actualMetadata[i], 1e-5);
        }

        double actualFirstGridVal = PixelFunctions.value(testRaster, 0, 0, 1);
        double expectedFirstGridVal = 60.95357131958008;
        assertEquals(expectedFirstGridVal, actualFirstGridVal, 1e-6);
    }

    @Test
    public void testNetCdfClassicLongForm() throws FactoryException, IOException, TransformException {
        GridCoverage2D testRaster = RasterConstructors.fromNetCDF(testNc, "O3", "lon", "lat");
        double[] expectedMetadata = {4.9375, 50.9375, 80, 48, 0.125, -0.125, 0, 0, 0, 4};
        double[] actualMetadata = RasterAccessors.metadata(testRaster);
        for (int i = 0; i < expectedMetadata.length; i++) {
            assertEquals(expectedMetadata[i], actualMetadata[i], 1e-5);
        }

        double actualFirstGridVal = PixelFunctions.value(testRaster, 0, 0, 1);
        double expectedFirstGridVal = 60.95357131958008;
        assertEquals(expectedFirstGridVal, actualFirstGridVal, 1e-6);
    }

//    @Test
//    public void testNetCdf4() throws FactoryException, IOException, TransformException {
//        GridCoverage2D testRaster = RasterConstructors.fromNetCDF(testBig, "abso4");
//        double[] expectedMetadata = {-0.9375, 89.5045, 192, 96, 1.875, -1.86467, 0, 0, 0, 8};
//        double[] actualMetadata = RasterAccessors.metadata(testRaster);
//        for (int i = 0; i < expectedMetadata.length; i++) {
//            assertEquals(expectedMetadata[i], actualMetadata[i], 1e-5);
//        }
//
//        double actualFirstGridVal = PixelFunctions.value(testRaster, 0, 0, 1);
//        double expectedFirstGridVal = 0;
//        assertEquals(expectedFirstGridVal, actualFirstGridVal, 1e-6);
//    }

    @Test
    public void testRecordInfo() throws IOException {
        String actualRecordInfo = RasterConstructors.getRecordInfo(testNc);
        String expectedRecordInfo = "O3(time=2, z=2, lat=48, lon=80)\n" +
                "\n" +
                "NO2(time=2, z=2, lat=48, lon=80)";
        assertEquals(expectedRecordInfo, actualRecordInfo);
    }



}
