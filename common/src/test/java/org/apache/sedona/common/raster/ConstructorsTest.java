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

import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class ConstructorsTest extends RasterTestBase {

    @Test
    public void fromArcInfoAsciiGrid() throws IOException, FactoryException {
        GridCoverage2D gridCoverage2D = Constructors.fromArcInfoAsciiGrid(arc.getBytes(StandardCharsets.UTF_8));

        Geometry envelope = Functions.envelope(gridCoverage2D);
        assertEquals(3600, envelope.getArea(), 0.1);
        assertEquals(378922d + 30, envelope.getCentroid().getX(), 0.1);
        assertEquals(4072345d + 30, envelope.getCentroid().getY(), 0.1);
        assertEquals(2, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(2, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(0d, gridCoverage2D.getSampleDimension(0).getNoDataValues()[0], 0.1);
        assertEquals(3d, gridCoverage2D.getRenderedImage().getData().getPixel(1, 1, (double[])null)[0], 0.1);
    }

    @Test
    public void fromGeoTiff() throws IOException, FactoryException {
        GridCoverage2D gridCoverage2D = Constructors.fromGeoTiff(geoTiff);

        Geometry envelope = Functions.envelope(gridCoverage2D);
        assertEquals(100, envelope.getArea(), 0.1);
        assertEquals(5, envelope.getCentroid().getX(), 0.1);
        assertEquals(5, envelope.getCentroid().getY(), 0.1);
        assertEquals(10, gridCoverage2D.getRenderedImage().getTileHeight());
        assertEquals(10, gridCoverage2D.getRenderedImage().getTileWidth());
        assertEquals(10d, gridCoverage2D.getRenderedImage().getData().getPixel(5, 5, (double[])null)[0], 0.1);
        assertEquals(4, gridCoverage2D.getNumSampleDimensions());
    }
}