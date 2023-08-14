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

import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

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

}
