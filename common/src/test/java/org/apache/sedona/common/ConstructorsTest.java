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

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import static org.junit.Assert.*;

public class ConstructorsTest {

    @Test
    public void geomFromWKT() throws ParseException {
        assertNull(Constructors.geomFromWKT(null, 0));

        Geometry geom = Constructors.geomFromWKT("POINT (1 1)", 0);
        assertEquals(0, geom.getSRID());
        assertEquals("POINT (1 1)", geom.toText());

        geom = Constructors.geomFromWKT("POINT (1 1)", 3006);
        assertEquals(3006, geom.getSRID());
        assertEquals("POINT (1 1)", geom.toText());

        ParseException invalid = assertThrows(ParseException.class, () -> Constructors.geomFromWKT("not valid", 0));
        assertEquals("Unknown geometry type: NOT (line 1)", invalid.getMessage());
    }
    @Test
    public void mLineFromWKT() throws ParseException {
        assertNull(Constructors.mLineFromText(null, 0));
        assertNull(Constructors.mLineFromText("POINT (1 1)", 0));
        Geometry geom = Constructors.mLineFromText("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", 0);
        assertEquals(0,geom.getSRID());
        assertEquals("MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))",geom.toText());

        geom = Constructors.mLineFromText("MULTILINESTRING((1 2, 3 4), (4 5, 6 7))", 3306);
        assertEquals(3306,geom.getSRID());
        assertEquals("MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))",geom.toText());

        ParseException invalid = assertThrows(ParseException.class, () -> Constructors.mLineFromText("MULTILINESTRING(not valid)", 0));
        assertEquals("Expected EMPTY or ( but found 'not' (line 1)", invalid.getMessage());

    }

    @Test
    public void mPolyFromWKT() throws ParseException {
        assertNull(Constructors.mPolyFromText(null, 0));
        assertNull(Constructors.mPolyFromText("POINT (1 1)", 0));
        Geometry geom = Constructors.mPolyFromText("MULTIPOLYGON(((0 0 ,20 0 ,20 20 ,0 20 ,0 0 ),(5 5 ,5 7 ,7 7 ,7 5 ,5 5)))", 0);
        assertEquals(0,geom.getSRID());
        assertEquals("MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))",geom.toText());

        geom = Constructors.mPolyFromText("MULTIPOLYGON(((0 0 ,20 0 ,20 20 ,0 20 ,0 0 ),(5 5 ,5 7 ,7 7 ,7 5 ,5 5)))", 3306);
        assertEquals(3306,geom.getSRID());
        assertEquals("MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))"
                ,geom.toText());

        IllegalArgumentException invalid = assertThrows(IllegalArgumentException.class,
                () -> Constructors.mPolyFromText("MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))", 0));
        assertEquals("Points of LinearRing do not form a closed linestring", invalid.getMessage());

        ParseException parseException = assertThrows(ParseException.class, () -> Constructors.mPolyFromText("MULTIPOLYGON(not valid)", 0));
        assertEquals("Expected EMPTY or ( but found 'not' (line 1)", parseException.getMessage());

    }
}