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
}