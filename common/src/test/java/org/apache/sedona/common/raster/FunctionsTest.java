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

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.operation.TransformException;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

public class FunctionsTest extends RasterTestBase {

    @Test
    public void envelope() {
        Geometry envelope = Functions.envelope(oneBandRaster);
        assertEquals(3600.0d, envelope.getArea(), 0.1d);
        assertEquals(378922.0d + 30.0d, envelope.getCentroid().getX(), 0.1d);
        assertEquals(4072345.0d + 30.0d, envelope.getCentroid().getY(), 0.1d);
    }

    @Test
    public void testNumBands() {
        assertEquals(1, Functions.numBands(oneBandRaster));
        assertEquals(4, Functions.numBands(multiBandRaster));
    }

    @Test
    public void value() throws TransformException {
        assertNull("Points outside of the envelope should return null.", Functions.value(oneBandRaster, point(1, 1), 1));
        assertNull("Invalid band should return null.", Functions.value(oneBandRaster, point(378923, 4072346), 0));
        assertNull("Invalid band should return null.", Functions.value(oneBandRaster, point(378923, 4072346), 2));

        Double value = Functions.value(oneBandRaster, point(378923, 4072346), 1);
        assertNotNull(value);
        assertEquals(2.0d, value, 0.1d);

        assertNull("Null should be returned for no data values.", Functions.value(oneBandRaster, point(378923, 4072376), 1));
    }

    @Test
    public void valueWithMultibandRaster() throws TransformException {
        // Multiband raster
        assertEquals(9d, Functions.value(multiBandRaster, point(4.5d,4.5d), 3), 0.1d);
        assertEquals(255d, Functions.value(multiBandRaster, point(4.5d,4.5d), 4), 0.1d);
    }

    @Test
    public void values() throws TransformException {
        // The function 'value' is implemented using 'values'.
        // These test only cover bits not already covered by tests for 'value'
        List<Geometry> points = Arrays.asList(new Geometry[]{point(378923, 4072346), point(378924, 4072346)});
        List<Double> values = Functions.values(oneBandRaster, points, 1);
        assertEquals(2, values.size());
        assertTrue(values.stream().allMatch(Objects::nonNull));

        values = Functions.values(oneBandRaster, points, 0);
        assertEquals(2, values.size());
        assertTrue("All values should be null for invalid band index.", values.stream().allMatch(Objects::isNull));

        values = Functions.values(oneBandRaster, points, 2);
        assertEquals(2, values.size());
        assertTrue("All values should be null for invalid band index.", values.stream().allMatch(Objects::isNull));

        values = Functions.values(oneBandRaster, Arrays.asList(new Geometry[]{point(378923, 4072346), null}), 1);
        assertEquals(2, values.size());
        assertNull("Null geometries should return null values.", values.get(1));
    }

    private Point point(double x, double y) {
        return new GeometryFactory().createPoint(new Coordinate(x, y));
    }
}