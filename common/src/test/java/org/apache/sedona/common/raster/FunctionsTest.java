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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.image.Raster;
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
        assertEquals(4326, RasterAccessors.envelope(oneBandRasterWithUpdatedSrid).getSRID());
        assertTrue(RasterAccessors.envelope(oneBandRasterWithUpdatedSrid).equalsTopo(RasterAccessors.envelope(oneBandRaster)));

        GridCoverage2D multiBandRasterWithUpdatedSrid = RasterEditors.setSrid(multiBandRaster, 0);
        assertEquals(0 , RasterAccessors.srid(multiBandRasterWithUpdatedSrid));
    }

    @Test
    public void value() throws TransformException {
        assertNull("Points outside of the envelope should return null.", PixelFunctions.value(oneBandRaster, point(1, 1), 1));
        assertNull("Invalid band should return null.", PixelFunctions.value(oneBandRaster, point(378923, 4072346), 0));
        assertNull("Invalid band should return null.", PixelFunctions.value(oneBandRaster, point(378923, 4072346), 2));

        Double value = PixelFunctions.value(oneBandRaster, point(378923, 4072346), 1);
        assertNotNull(value);
        assertEquals(2.0d, value, 0.1d);

        assertNull("Null should be returned for no data values.", PixelFunctions.value(oneBandRaster, point(378923, 4072376), 1));
    }

    @Test
    public void valueWithMultibandRaster() throws TransformException {
        // Multiband raster
        assertEquals(9d, PixelFunctions.value(multiBandRaster, point(4.5d,4.5d), 3), 0.1d);
        assertEquals(255d, PixelFunctions.value(multiBandRaster, point(4.5d,4.5d), 4), 0.1d);
    }

    @Test
    public void testPixelAsPointUpperLeft() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(1, 5, 10, 123, -230, 8);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 0, 0);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(123, coordinates.x, 1e-9);
        assertEquals(-230, coordinates.y, 1e-9);
    }

    @Test
    public void testPixelAsPointMiddle() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 10, 123, -230, 8);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 2, 4);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(139, coordinates.x, 1e-9);
        assertEquals(-262, coordinates.y, 1e-9);
    }

    @Test
    public void testPixelAsPointRandom() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 10, 123, -230, 8);
        Geometry actualPoint = PixelFunctions.getPixelAsPoint(emptyRaster, 4, 1);
        Coordinate coordinates = actualPoint.getCoordinate();
        assertEquals(155, coordinates.x, 1e-9);
        assertEquals(-238, coordinates.y, 1e-9);
    }

    @Test
    public void testPixelAsPointOutOfBounds() throws FactoryException, TransformException {
        GridCoverage2D emptyRaster = RasterConstructors.makeEmptyRaster(2, 5, 10, 123, -230, 8);
        Exception e = assertThrows(IllegalArgumentException.class, () -> PixelFunctions.getPixelAsPoint(emptyRaster, 5, 1));
        String expectedMessage = "Specified pixel coordinates do not lie in the raster";
        assertEquals(expectedMessage, e.getMessage());

    }

    @Test
    public void values() throws TransformException {
        // The function 'value' is implemented using 'values'.
        // These test only cover bits not already covered by tests for 'value'
        List<Geometry> points = Arrays.asList(new Geometry[]{point(378923, 4072346), point(378924, 4072346)});
        List<Double> values = PixelFunctions.values(oneBandRaster, points, 1);
        assertEquals(2, values.size());
        assertTrue(values.stream().allMatch(Objects::nonNull));

        values = PixelFunctions.values(oneBandRaster, points, 0);
        assertEquals(2, values.size());
        assertTrue("All values should be null for invalid band index.", values.stream().allMatch(Objects::isNull));

        values = PixelFunctions.values(oneBandRaster, points, 2);
        assertEquals(2, values.size());
        assertTrue("All values should be null for invalid band index.", values.stream().allMatch(Objects::isNull));

        values = PixelFunctions.values(oneBandRaster, Arrays.asList(new Geometry[]{point(378923, 4072346), null}), 1);
        assertEquals(2, values.size());
        assertNull("Null geometries should return null values.", values.get(1));
    }

    private Point point(double x, double y) {
        return new GeometryFactory().createPoint(new Coordinate(x, y));
    }
}