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

import org.apache.sedona.common.utils.GeomUtils;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GeometryUtilTest {
    public static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private Coordinate[] coordArray(double... coordValues) {
        Coordinate[] coords = new Coordinate[(int)(coordValues.length / 2)];
        for (int i = 0; i < coordValues.length; i += 2) {
            coords[(int)(i / 2)] = new Coordinate(coordValues[i], coordValues[i+1]);
        }
        return coords;
    }

    @Test
    public void extractGeometryCollection() throws ParseException, IOException {
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(
                new Polygon[] {
                        GEOMETRY_FACTORY.createPolygon(coordArray(0, 1,3, 0,4, 3,0, 4,0, 1)),
                        GEOMETRY_FACTORY.createPolygon(coordArray(3, 4,6, 3,5, 5,3, 4))
                }
        );
        Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(5.0, 8.0));
        GeometryCollection gc1 = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {
                multiPolygon, point
        });
        GeometryCollection gc = GEOMETRY_FACTORY.createGeometryCollection(
                new Geometry[] {
                        multiPolygon.copy(),
                        gc1
                }
        );
        List<Geometry> geoms = GeomUtils.extractGeometryCollection(gc);
        assert (
                Objects.equals(
                        GeomUtils.getWKT(
                                GEOMETRY_FACTORY.createGeometryCollection(
                                        geoms.toArray(new Geometry[geoms.size()]))),
                        "GEOMETRYCOLLECTION (POLYGON ((0 1, 3 0, 4 3, 0 4, 0 1)), POLYGON ((3 4, 6 3, 5 5, 3 4)), POINT (5 8), POLYGON ((0 1, 3 0, 4 3, 0 4, 0 1)), POLYGON ((3 4, 6 3, 5 5, 3 4)))"
                )
        );

    }


}
